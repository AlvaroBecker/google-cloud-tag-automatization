from google.cloud import datacatalog_v1
import os
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import funciones
import time
import json

# Set the path to the JSON file containing the service account key

path_to_credentials =os.environ["GOOGLE_APPLICATION_CREDENTIALS"] 
# Load the service account key
credentials = Credentials.from_service_account_file(path_to_credentials)
dclient = datacatalog_v1.DataCatalogClient(credentials=credentials)
client = bigquery.Client()
# Set the project_id, tag_template, and location variables
project_id = os.environ["PROJECT_ID"]
tag_template = os.environ["TAG_TEMPLATE"]
location = os.environ["LOCATION"]
template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template}"

diccionario = os.environ["DICCIONARIO"]

start_time = time.time()

check={}
"""
TO TEST THIS PROJECT YOU NEED TO DELETE THE TAGS FROM THE TABLES 
AND DATASETS YOU WANT TO USE FOR TESTING, YOU CAN ADD A DICCTIONARY ON THE ENVIRONMENT VARIABLES
OR CREATE A JSON FILE AND LOAD IT ON THE MAIN.PY FILE
"""


"""
#Open the json file and load it as a dictionary and comment the first "diccionario" variable
with open('dictionary.json', 'r') as file:
    diccionario = json.load(file)
"""
datasets_tables=funciones.get_datasets_list(client)
tag_fields_list=funciones.get_templates_fields(dclient, template_name)

funciones.tag_table(project_id, dclient, location, tag_template, tag_fields_list, datasets_tables, diccionario, template_name,client)

end_time = time.time()
execution_time = end_time - start_time
print(execution_time)       