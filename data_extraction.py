import database_utils
from sqlalchemy import inspect, text
import pandas as pd
import tabula

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


if __name__ == "__main__":
    ## Code testing the functionality 
    dbConnector = database_utils.DatabaseConnector()
    extractor = DataExtractor()

    df = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")