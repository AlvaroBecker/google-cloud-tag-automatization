from google.cloud import datacatalog_v1
import re
import logging

logging.basicConfig(level=logging.INFO)
"""
Falta agregar una logica para mantener ciertos tags que se originan
en el nombre del dataset para agregarlos por default a las tablas, para esto
consideren revisar las funciones tag_table y check_strings
-----
A logic needs to be added to maintain certain labels that originate
in the data set name to add them by default to the tables, for this
consider reviewing the tag_table and check_strings functions
"""
def get_datasets_list(client):
    """Retrieve a list of datasets and their respective tables.
    
    Args:
        client: BigQuery client instance.
        
    Returns:
        dict: A dictionary with dataset names as keys and a list of table names as values.
    """
    logging.info("Fetching datasets and tables...")
    datasets_list={}
    datasets = client.list_datasets()
    for dataset in datasets:
        tables = client.list_tables(dataset)
        datasets_list[f'{dataset.dataset_id}']=[]
        for table in tables:
           datasets_list[f'{dataset.dataset_id}'].append(f"{table.table_id}")
    logging.info(f"Found {len(datasets_list)} datasets.")
    return datasets_list

def get_templates_fields(client, template_name):
    """List the possible data types in a dictionary.
    
    Args:
        client: DataCatalog client instance.
        template_name (str): Name of the tag template.
        
    Returns:
        dict: A dictionary mapping field names to their data types.
    """
    logging.info(f"Fetching fields for template: {template_name}...")
    tag_template = client.get_tag_template(name=template_name)
    fields = tag_template.fields
    field_value = {
        'STRING': 'string_value',
        'DOUBLE': 'double_value',
        'BOOL': 'bool_value',
        'TIMESTAMP': 'timestamp_value',
        'ENUM': 'enum_value.display_name',
        'RECORD': 'record_value'
    }
    properties_type = {
        'primitive_type': ['STRING', 'BOOL', 'DOUBLE'],
        'enum_type': 'ENUM'
    }
    fields_list = {}
    for field_id, field in fields.items():
        fields_list[field_id] = field_value[get_field_type(field.type_, properties_type)]
    return fields_list

def get_field_type(field_type, properties_type):
    """Retrieve the type of a field.
    
    Args:
        field_type: Type of the field.
        properties_type (dict): Dictionary of properties and their types.
        
    Returns:
        str: The type of the field.
    """
    if not isinstance(field_type, datacatalog_v1.FieldType):
        return "Invalid field type"
    for prop, val in properties_type.items():
        if getattr(field_type, prop):
            if type(val) == str:
                return val
            elif type(val) == list:
                if str(getattr(field_type, prop)).split('.')[1] in val:
                    return str(getattr(field_type, prop)).split('.')[1]

def initialize_template_fields(client, template_name):
    """Initialize a dictionary with the fields of a template.
    
    Args:
        client: DataCatalog client instance.
        template_name (str): Name of the tag template.
        
    Returns:
        dict: A dictionary with initialized fields of the template.
    """
    fields = get_templates_fields(client, template_name)
    initialized_dict = {field: None for field in fields.keys()}
    return initialized_dict


def check_year_in_string(s, client, dataset_name=None):
    """Check if a string contains a valid year.
    
    Args:
        s (str): Input string.
        
    Returns:
        str: The year if found, otherwise "MISSING".
    """
    # Search a pattern of 4 digits that represents a year from 1900 and 2099
    match = re.search(r'20\d{2}|19\d{2}', s)
    if match:
        return match.group()
    else:
        if dataset_name:  # If a dataset_name is provided, it means s is a table name
            try:
                table_ref = client.dataset(dataset_name).table(s)
                table = client.get_table(table_ref)
                return str(table.created.year)
            except:
                return "MISSING"
        else:  # s is a dataset name
            try:
                dataset_ref = client.dataset(s)
                dataset = client.get_dataset(dataset_ref)
                return str(dataset.created.year)
            except:
                return "MISSING"
def get_default_enum_value(template_name):
    """Retrieve the first allowed value for enum_type fields.
    
    Args:
        template_name (str): Name of the tag template.
        
    Returns:
        dict: A dictionary with default enum values for each field.
    """
    client = datacatalog_v1.DataCatalogClient()
    template = client.get_tag_template(name=template_name)
    default_values = {}
    for field, details in template.fields.items():
        if hasattr(details.type_, "enum_type") and details.type_.enum_type.allowed_values:
            allowed_values = details.type_.enum_type.allowed_values
            if allowed_values:  
                default_value = allowed_values[0].display_name
                default_values[field] = default_value  
    return default_values


def extract_numbers_from_string(s):
    """Extract all numbers from a string.
    
    Args:
        s (str): Input string.
        
    Returns:
        list: A list of extracted numbers.
    """
    return re.findall(r'\d+', s)

def get_required_fields(template_name):
    """Retrieve the required fields of a template and their possible enum values.
    
    Args:
        template_name (str): Name of the tag template.
        
    Returns:
        dict: A dictionary with required fields and their possible enum values.
    """
    client = datacatalog_v1.DataCatalogClient()
    template = client.get_tag_template(name=template_name)
    required_fields = {}
    for field, details in template.fields.items():
        if details.is_required:
            if hasattr(details.type_, "enum_type"):
                required_fields[field] = [enum_value.display_name for enum_value in details.type_.enum_type.allowed_values]
            else:
                required_fields[field] = ""
    return required_fields


def validate_result(result, required_fields):
    """Validate that the result contains all required fields and their values are valid.
    
    Args:
        result (dict): Result dictionary.
        required_fields (dict): Dictionary of required fields and their possible values.
        
    Returns:
        dict: Validated result.
    """
    
    for field, possible_values in required_fields.items():
        if field not in result or result[field] is None or result[field] == "":
            result[field] = "MISSING"
        elif possible_values:  
            if result[field] not in possible_values:
                result[field] = "ERROR"
    
    return result



def check_strings(dictionaries, strings, project_name, template_name, client,dataset_name=None):
    """Check the format of each dataset and table.
    
    Args:
        dictionaries (list): List of dictionaries with possible values.
        strings (str): Input string.
        project_name (str): Name of the project.
        template_name (str): Name of the tag template.
        
    Returns:
        dict: A dictionary with extracted values from the input string.
    """
    logging.info(f"Checking format for string: {strings}...")
    parts = strings.split("_")
    result = {key: "" for d in dictionaries for key in d.keys()}
    
    for part in parts:
        for dictionary in dictionaries:
            for key, values in dictionary.items():
                for subkey, subvalues in values.items():
                    for sv in subvalues:
                        if re.fullmatch(sv.lower(), part.lower()):
                            result[key] = subkey
                            break

    # Extract numbers from the string
    numbers = extract_numbers_from_string(strings)
    
    if result["business"]:
        for num in numbers:
            if num in dictionaries[0]["business"].get(result["business"], []):
                result["society"] = num
                break
       
    # Verify if the string contains a valid year
    if dataset_name:
        result["year"] = check_year_in_string(strings, client, dataset_name)
    else:
        result["year"] = check_year_in_string(strings, client)
    
    # Stablish the project name
    result["project"] = project_name

    default_values = get_default_enum_value(template_name)
    result = {k: default_values.get(k, result[k]) if not result[k] else result[k] for k in result}
    # Validation process
    try:
        required_fields = get_required_fields(template_name)
        validated_result = validate_result(result, required_fields)
    except Exception as e:
        logging.error(f"Error during validation: {e}")
        return None

    return validated_result

def tag_table(project_id, datacatalog_client, location, tag_template, tag_fields_list, datasets_tables, dictionaries, template_name,client):
    """Tag a table based on its format.
    
    Args:
        project_id (str): Project ID.
        datacatalog_client: DataCatalog client instance.
        location (str): Location of the tag template.
        tag_template (str): Name of the tag template.
        tag_fields_list (dict): Dictionary of tag fields.
        datasets_tables (dict): Dictionary of datasets and their tables.
        dictionaries (list): List of dictionaries with possible values.
        template_name (str): Name of the tag template.
    """
    logging.info("Starting the tagging process...")
    diccionario = dictionaries
    formatos_tag = {}
    for dataset_id, table_id in datasets_tables.items(): 
        formatos_tag[dataset_id] = check_strings(diccionario, dataset_id, project_id, template_name,client)
        
        if isinstance(table_id, list):
            for datatable_id in table_id:
                formatos_tag[datatable_id] = formatos_tag[datatable_id] = check_strings(diccionario, datatable_id, project_id, template_name, client, dataset_name=dataset_id)
        else:
            formatos_tag[table_id] = check_strings(diccionario, table_id, project_id, template_name,client,dataset_name=dataset_id)
    logging.info(f"Tags a agregar: {formatos_tag}")  
    try:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}"
        table_entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})
    except:
        resource_name = f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        table_entry = datacatalog_client.lookup_entry(request={"linked_resource": resource_name})
    
    tag_template_name = f'projects/{project_id}/locations/{location}/tagTemplates/{tag_template}'
    tag = datacatalog_v1.types.Tag()
    tag.template = tag_template_name

    for fields in tag_fields_list.keys():
        tag.fields[f'{fields}'] = datacatalog_v1.types.TagField()

    tag_setters = {
        'string_value': lambda field, value: setattr(field, 'string_value', value),
        'double_value': lambda field, value: setattr(field, 'double_value', value),
        'bool_value': lambda field, value: setattr(field, 'bool_value', str(value).lower() == 'true'),
        'timestamp_value': lambda field, value: setattr(field, 'timestamp_value', value),
        'enum_value.display_name': lambda field, value: setattr(field.enum_value, 'display_name', value),
        'record_value': lambda field, value: setattr(field, 'record_value', value)
    }

    for tabla_dataset, dic_tagfields in formatos_tag.items():
        tag.name = f'{tabla_dataset}'
        for tagfield, tagtype in tag_fields_list.items():
            valor = formatos_tag[tabla_dataset].get(tagfield, None)
            if valor is not None and valor not in ["ERROR", "MISSING"] and tagtype in tag_setters:
                setter_function = tag_setters[tagtype]
                setter_function(tag.fields[tagfield], valor)

        if any(field for field in tag.fields.values() if field is not None):

            try:
                datacatalog_client.create_tag(parent=table_entry.name, tag=tag)
                logging.info(f"Created tag: {tag_template} for {tabla_dataset}")
            except Exception as e:
                logging.error(f"Error creating tag for {tabla_dataset}: {e}")
                
        else:
            logging.warning(f"Skipped tag creation for {tabla_dataset} as it does not have any valid fields.")