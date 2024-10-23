# Multinational Retail Data Centralisation Project

## Description
This project looks at extracting and cleaning data from various sources (database, JSON, s3 bucket, API, PDF), then storing the cleaned data in a local database.

## Installation
This project requires libraries `sqlalchemy`, `pandas`, `tabula`, `boto3`, `requests`, and `dateutil`, all of which can be installed using `pip`.

## Usage
`database_utils.py` provides a DatabaseConnector class that can be used to create SQLAlchemy connections to databases, given that the details of it are stored locally in yaml. `init_db_engine` reads the local credentials then creates and returns a connection to a database. `upload_to_db` takes a dataframe and table name, then uploads the data to a local database (defined by credentials), replacing the data if the table already exists.

`data_extraction.py` provides a DataExtractor class that provides methods for extracting many forms of data into dataframes. `list_db_tables` takes a DatabaseConnection instance and returns all table names. `read_rds_table` takes a DatabaseConnection instance and a table name, and returns the data from that table as a dataframe. `retrieve_pdf_data` takes a url to a pdf table and returns the table as a dataframe. `extract_from_s3` takes an s3 csv link, then returns the data from that csv as a dataframe. `extract_json` takes a url to a json file, then returns it as a dataframe.

## Structure
`database_utils.py` handles all of the connections to the databases (for extracting data, and then storing locally), making use of local credentials stored in yaml.

`data_extraction.py` handles extracting data from different sources, mentioned in the introduction. The data from each of these methods is returned as dataframes that need cleaning.

`data_cleaning.py` handles the cleaning of specific pieces of data. These methods aren't created to be reused in different scenarios as they reference specific pieces of data and perform cleaning that's tailored to that data.
