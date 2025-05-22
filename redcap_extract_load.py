# Packages
import requests
import pandas as pd
import numpy as np
from io import StringIO
import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine

# Start by retrieving the RECap project URL and Token from a secret '.env' file
# (see brief example at: https://drivendata.github.io/cookiecutter-data-science/#keep-secrets-and-configuration-out-of-version-control)
dotenv_path = find_dotenv() # Find .env automagically by walking up directories until it's found
load_dotenv(dotenv_path) # Load up the entries as environment variables
database_url = os.environ.get("redcap_url")
redcap_token = os.environ.get("redcap_token")

# Metadata (to create variable dictionary)
metadata_request = {
    'token': redcap_token,
    'content': 'metadata',
    'format': 'csv',
    'returnFormat': 'csv'
}
metadata = requests.post(database_url,data=metadata_request)
metadata = pd.read_csv(StringIO(metadata.text))
# Obtain list with all form names
form_names = metadata.form_name.unique().tolist()

# Form-Event Mappings
formEventMapping_request = {
    'token': redcap_token,
    'content': 'formEventMapping',
    'format': 'csv',
    'returnFormat': 'csv'
}
formEventMapping = requests.post(database_url,data=formEventMapping_request)
formEventMapping = pd.read_csv(StringIO(formEventMapping.text))

# Dictionary to map forms with events
form_event_dict = {}
for form_name in form_names:
    this_event = formEventMapping.loc[formEventMapping.form==form_name].unique_event_name.tolist()
    form_event_dict[form_name] = this_event

# Create empty dictionary to store REDCap forms
redcap_data_dict = {}

for form_name in form_names:
    # Create Form dictionary to request specific form to REDCap i.e. {'forms[0]':'baseline'}
    form_request = {'forms[0]':form_name}

    # We identify the events corresponding to the current form
    events_in_form = form_event_dict.get(form_name)
    events_in_form_len = len(events_in_form) # calculate number of events of the current form
    # Create Event dictionary with the events of the current form i.e.  {'events[0]':'visit_1_arm_1','events[1]':'visit_2_arm_1', ... }
    events_request = {'events[{}]'.format(i): events_in_form[i] for i in range(events_in_form_len)}

    # Dictionary with the preset settings that will be requested to REDCap
    data_request = {
        'token': redcap_token,
        'content': 'record',
        'action': 'export',
        'format': 'csv',
        'type': 'flat',
        'csvDelimiter': '',
        'rawOrLabel': 'label',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'true',
        'returnFormat': 'csv'
    }
    # Add the current form and corresponding events to the current REDCap 'data_request' dictionary
    data_request.update(form_request)
    data_request.update(events_request)
    # Now we perform the call to the REDCap API using the created request dictionary 'data_request'
    r = requests.post(database_url, data=data_request)

    # Import the obtained REDCap data into pandas dataframe
    # before creating the dataframe we check whether the current form has any content
    if r.text == '\n':
        pass # if it has no content no df is created
    else:
        r = pd.read_csv(StringIO(r.text))

    # We store this dataframe into a dictionary that uses the form name as a key
    redcap_data_dict[form_name] = r
    
    # Now we dynamically assign the current df to the 'form_name' value as the object name of the df
    exec(f"{form_name} = r")

# SQL Server connection parameters
eixampleclinic_server = os.getenv("eixampleclinic_server")     # Server name from env
eixampleclinic_port = os.getenv("eixampleclinic_port")         # SQL Server port
eixampleclinic_database = os.getenv("eixampleclinic_database") # Database name from env
eixampleclinic_username = os.getenv("eixampleclinic_username") # Username from env
eixampleclinic_password = os.getenv("eixampleclinic_password") # Password from env

# SQLAlchemy connection string to MariaDB database
connection_string = (
    f"mariadb+pymysql://{eixampleclinic_username}:{eixampleclinic_password}"
    f"@{eixampleclinic_server}:{eixampleclinic_port}/{eixampleclinic_database}"
)

# Installing pymysql is required for conection with MariaDB (conda install -c conda-forge pymysql)

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

# Save DFs to database
for this_form_name in form_names:
    
    this_df = locals().get(this_form_name)
    
    this_df.to_sql('breast_redcap_'+this_form_name,
                         con=engine, #sqlalchemy.engine.(Engine or Connection) or sqlite3.Connection
                        #  schema='raw',
                         if_exists='replace', #merge (update, insert, delete)
                         index=False)


for form_name, df in redcap_data_dict.items():
    
    if not isinstance(df, pd.DataFrame):
        print(f"Skipping non DataFrame object: {form_name}")
        continue
    
    df.to_sql('breast_redcap_' + form_name,
              con=engine, #sqlalchemy.engine.(Engine or Connection) or sqlite3.Connection
              #  schema='raw',
              if_exists='replace', #merge (update, insert, delete)
              index=False)