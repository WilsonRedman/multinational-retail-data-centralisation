import database_utils, api_keys
from sqlalchemy import inspect, text
import pandas as pd
import tabula
import requests

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


if __name__ == "__main__":
    ## Code testing the functionality 
    dbConnector = database_utils.DatabaseConnector()
    extractor = DataExtractor()

    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    response = extractor.list_number_of_stores(url, api_keys.storeHeader)

    url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    response = extractor.retrieve_stores_data(url, api_keys.storeHeader, response)
    print(response)