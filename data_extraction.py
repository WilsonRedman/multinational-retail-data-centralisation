import database_utils, api_keys
from sqlalchemy import inspect, text
import pandas as pd
import tabula, requests, boto3

class DataExtractor:
    
    def list_db_tables(self, dbConnector):
        engine = dbConnector.init_db_engine()
        inspector = inspect(engine)

        return inspector.get_table_names()
    
    def read_rds_table(self, table, dbConnector):
        engine = dbConnector.init_db_engine()
        data = pd.read_sql_table(table, engine)

        return data
    
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, stream = True, pages = "all", lattice = True)
        df = pd.concat(dfs, ignore_index = True)

        return df
    
    def list_number_of_stores(self, url, headers):
        response = requests.get(url, headers = headers)

        return response.json()["number_stores"]
    
    def retrieve_stores_data(self, url, headers, num):
        stores = pd.DataFrame()

        for i in range (0, num):
            tempUrl = url+str(i)
            response = requests.get(tempUrl, headers = headers)

            df = pd.json_normalize(response.json())
            stores = pd.concat([stores, df])

        return stores
    
    def extract_from_s3(self, link):
        s3 = boto3.client("s3")
        bucket = link[5:].split("/", 1)[0]
        file = link[5:].split("/", 1)[1]

        csv = s3.get_object(Bucket = bucket, Key = file)
        df = pd.read_csv(csv["Body"], index_col=0)
        
        return df


if __name__ == "__main__":
    ## Code testing the functionality 
    dbConnector = database_utils.DatabaseConnector()
    extractor = DataExtractor()

    url = "s3://data-handling-public/products.csv"
    print(extractor.extract_from_s3(url))