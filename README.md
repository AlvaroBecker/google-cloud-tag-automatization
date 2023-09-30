# google-cloud-tag-automatization

Automatization of tagging using Python and GCP tools.

## Description

This project aims to automate the process of tagging datasets and tables within Google Cloud Platform using Python scripts and various GCP tools.

## Prerequisites

- Google Cloud SDK
- Python 3.6+
- `google-cloud-datacatalog` library
- `google-cloud-bigquery` library
- `dotenv` library

## Setup

1. Ensure you have `google-cloud-sdk` installed and configured with necessary permissions.
2. Install the required Python libraries:

```bash
pip install google-cloud-datacatalog google-cloud-bigquery python-dotenv
```

1. Set up your environment variables. You can use a .env file for this:

```env
GOOGLE_APPLICATION_CREDENTIALS=path_to_your_service_account_key.json
PROJECT_ID=your_project_id
TAG_TEMPLATE=your_tag_template
LOCATION=your_location
DICCIONARIO=your_dictionary
```

Load the environment variables using:

```Python
from dotenv import load_dotenv
load_dotenv()
```

## Files

# 1. `funciones.py`

Contains various utility functions used for:

* Fetching datasets and their respective tables.
* Listing possible data types for a tag template.
* Checking string formats and extracting specific values like year or numbers.
* Tagging tables based on their format and more.

# 2. `delete_tags.py`

This script is used to delete tags from datasets and tables within Google Cloud Platform.

# 3. `main.py`

The main script that orchestrates the entire tagging process. It makes use of functions from `funciones.py` to achieve its goal.

## Usage
1. First, if you want to delete existing tags, run:

```bash
python delete_tags.py 
```

1. To execute the main tagging process:

```bash
python main.py
```

## Important Notes

* To test this project, make sure to delete the tags from the tables and datasets you want to use for testing. You can add a dictionary in the environment variables or create a JSON file and load it in main.py.

* Always make sure to have the necessary permissions when trying to delete or add tags.
Contribution

Feel free to contribute to this project by opening issues or submitting pull requests.

