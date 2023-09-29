import os
from google.cloud import datacatalog_v1
from google.cloud import bigquery
from dotenv import load_dotenv
import logging
import funciones  
from google.oauth2.service_account import Credentials


logging.basicConfig(level=logging.INFO)

load_dotenv()

from google.cloud import datacatalog_v1

from google.cloud import datacatalog_v1

def delete_tags_from_resource(datacatalog_client, project_id, dataset_name, table_name=None, tag_template_name=None):
    """
    Delete all tags from a resource (dataset or table).

    Args:
        datacatalog_client (datacatalog_v1.DataCatalogClient): Data Catalog client. 
        project_id (str): ID of the GCP project.
        dataset_name (str): Dataset name.
        table_name (str, optional): Table name (optional).
        tag_template_name (str, optional): Complete name of the tag template.
    """

    # Construct a fully qualified dataset name and table name (if present).
    if table_name:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_name}/tables/{table_name}"
    else:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_name}"

    # Find the entry to delete.
    entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})

    # List of every tag template associated with the entry.
    tags = datacatalog_client.list_tags(parent=entry.name)
    for tag in tags:
        print(f"Found tag for resource {resource_name}: {tag.name}")
        if not tag_template_name or tag.template == tag_template_name:
            datacatalog_client.delete_tag(name=tag.name)
            print(f"Deleted tag: {tag.name}")

project_id = os.getenv('PROJECT_ID')
location = os.getenv('LOCATION')
template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{os.getenv('TAG_TEMPLATE')}"
credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
credentials = Credentials.from_service_account_file(credentials_path)
client = datacatalog_v1.DataCatalogClient(credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)

# Obtain datasets and tables
datasets_tables = funciones.get_datasets_list(bigquery_client)

for dataset, tables in datasets_tables.items():
    print("deleting from datasets")
    delete_tags_from_resource(client, project_id, dataset, tag_template_name=template_name)
    for table in tables:
        print("deleting from tables")
        delete_tags_from_resource(client, project_id, dataset, table_name=table, tag_template_name=template_name)  
