from sqlalchemy import inspect
import database_utils
import tabula, requests, boto3, pandas as pd

class DataExtractor:
    
    def list_db_tables(self, dbConnector):
        '''
        Lists all tables in the database connected to

        Args:
            dbConnector (DatabaseConnector): Instance of DatabaseConnector from the database_utils.py file

        Returns:
            List: Containing all table names in the connected to database
        '''

        engine = dbConnector.init_db_engine()
        inspector = inspect(engine)

        return inspector.get_table_names()
    
    def read_rds_table(self, table, dbConnector):
        '''
        Retrieves data from a database table

        Args:
            table (string): Table name to read from
            dbConnector (DatabaseConnector): Instance of DatabaseConnector from the database_utils.py file

        Returns:
            DataFrame : Containing all data from the table
        '''

        engine = dbConnector.init_db_engine()
        data = pd.read_sql_table(table, engine)

        return data
    
    def retrieve_pdf_data(self, link):
        '''
        Retrieves data from a pdf file containing information in a table

        Args:
            link (string): Link to the pdf file to read from

        Returns:
            DataFrame : Containing all data in the pdf table
        '''
                
        dfs = tabula.read_pdf(link, stream = True, pages = "all", lattice = True)
        df = pd.concat(dfs, ignore_index = True)

        return df
    
    def list_number_of_stores(self, url, headers):
        '''
        Retrieves the number of stores argument from an API

        Args:
            url (string): API url
            headers (dictionary): Contains the headers for the API requests, including the key

        Returns:
            integer
        '''
           
        response = requests.get(url, headers = headers)

        return response.json()["number_stores"]
    
    def retrieve_stores_data(self, url, headers, num):
        '''
        Retrieves data from store APIs where each endpoint has an integer increasing as the last part

        Args:
            url (string): API url
            headers (dictionary): Contains the headers for the API requests, including the key
            num (int): Is the number of stores that are being looked at

        Returns:
            DataFrame: Containing all store data
        '''
        stores = pd.DataFrame()

        for i in range (0, num):
            tempUrl = url+str(i)
            response = requests.get(tempUrl, headers = headers)

            df = pd.json_normalize(response.json())
            stores = pd.concat([stores, df])

        return stores
    
    def extract_from_s3(self, url):
        '''
        Retrieves data from a csv file stored in an S3 bucket

        Args:
            url (string): The URL of the s3 bucket and the csv file in the bucket

        Returns:
            DataFrame : Containing all data in the csv file
        '''

        s3 = boto3.client("s3")
        bucket = url[5:].split("/", 1)[0]
        file = url[5:].split("/", 1)[1]

        csv = s3.get_object(Bucket = bucket, Key = file)
        df = pd.read_csv(csv["Body"], index_col=0)
        
        return df
    
    def extract_json(self, url):
        '''
        Retrieves data from a json file

        Args:
            url (string): The URL of the json file

        Returns:
            DataFrame : Containing all data in the json file
        '''

        df = pd.read_json(url)
        return df


if __name__ == "__main__":
    ## Code testing the functionality 
    dbConnector = database_utils.DatabaseConnector()
    extractor = DataExtractor()

    url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"

    print(extractor.extract_json(url))