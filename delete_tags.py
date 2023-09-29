import os
from google.cloud import datacatalog_v1
from google.cloud import bigquery
from dotenv import load_dotenv
import logging
import funciones  
from google.oauth2.service_account import Credentials

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Carga las variables de entorno del archivo .env
load_dotenv()

from google.cloud import datacatalog_v1

from google.cloud import datacatalog_v1

def delete_tags_from_resource(datacatalog_client, project_id, dataset_name, table_name=None, tag_template_name=None):
    """
    Elimina todas las etiquetas asociadas a un tag template específico.

    Args:
        datacatalog_client (datacatalog_v1.DataCatalogClient): Cliente de Data Catalog.
        project_id (str): ID del proyecto de GCP.
        dataset_name (str): Nombre del dataset.
        table_name (str, optional): Nombre de la tabla. Si es None, se asume que el recurso es un dataset.
        tag_template_name (str, optional): Nombre completo del tag template. Si es None, se eliminan todas las etiquetas.
    """

    # Construye el nombre del recurso
    if table_name:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_name}/tables/{table_name}"
    else:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_name}"

    # Busca la entrada correspondiente al recurso.
    entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})

    # Lista todas las etiquetas asociadas con esa entrada.
    tags = datacatalog_client.list_tags(parent=entry.name)
    for tag in tags:
        print(f"Found tag for resource {resource_name}: {tag.name}")
        if not tag_template_name or tag.template == tag_template_name:
            datacatalog_client.delete_tag(name=tag.name)
            print(f"Deleted tag: {tag.name}")
# Uso de la función
project_id = os.getenv('PROJECT_ID')
location = os.getenv('LOCATION')
template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{os.getenv('TAG_TEMPLATE')}"
credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
credentials = Credentials.from_service_account_file(credentials_path)
client = datacatalog_v1.DataCatalogClient(credentials=credentials)
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)

# Obtiene la lista de datasets y tablas
datasets_tables = funciones.get_datasets_list(bigquery_client)
#datasets_tables={"biop_cl_117_op_inv_dl_databus_rules_cordillera_dev": []}
#datasets_tables={"biop_cl_117_op_inv_dl_raw_metadata_cordillera_dev": ["bq_tb_pi_raw_metadata_cordillera"]}

# Uso de la función
project_id = os.getenv('PROJECT_ID')
template_name = f"projects/{project_id}/locations/{location}/tagTemplates/{os.getenv('TAG_TEMPLATE')}"

for dataset, tables in datasets_tables.items():
    print("deleting from datasets")
    delete_tags_from_resource(client, project_id, dataset, tag_template_name=template_name)
    for table in tables:
        print("deleting from tables")
        delete_tags_from_resource(client, project_id, dataset, table_name=table, tag_template_name=template_name)  # Elimina etiquetas de la tabla

