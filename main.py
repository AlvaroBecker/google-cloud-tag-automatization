import json
import os
import time

from google.cloud import bigquery, datacatalog_v1
from google.oauth2.service_account import Credentials

import funciones

# Constants
PATH_TO_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
PROJECT_ID = os.environ["PROJECT_ID"]
TAG_TEMPLATE = os.environ["TAG_TEMPLATE"]
LOCATION = os.environ["LOCATION"]
TEMPLATE_NAME = f"projects/{PROJECT_ID}/locations/{LOCATION}/tagTemplates/{TAG_TEMPLATE}"
DICCIONARIO = os.environ["DICCIONARIO"]


def main():
    """Main function."""
    start_time = time.time()

    # Load the service account key
    credentials = Credentials.from_service_account_file(PATH_TO_CREDENTIALS)
    dclient = datacatalog_v1.DataCatalogClient(credentials=credentials)
    client = bigquery.Client()

    """
    TO TEST THIS PROJECT YOU NEED TO DELETE THE TAGS FROM THE TABLES 
    AND DATASETS YOU WANT TO USE FOR TESTING, YOU CAN ADD A DICTIONARY ON THE ENVIRONMENT VARIABLES
    OR CREATE A JSON FILE AND LOAD IT ON THE MAIN.PY FILE
    """

    # Uncomment to load the dictionary from a JSON file
    # with open('dictionary.json', 'r') as file:
    #     diccionario = json.load(file)

    datasets_tables = funciones.get_datasets_list(client)
    tag_fields_list = funciones.get_templates_fields(dclient, TEMPLATE_NAME)

    funciones.tag_table(PROJECT_ID, dclient, LOCATION, TAG_TEMPLATE, tag_fields_list, datasets_tables, DICCIONARIO, TEMPLATE_NAME, client)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
