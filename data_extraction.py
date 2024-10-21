import database_utils
from sqlalchemy import inspect, text
import pandas as pd
import tabula

class DataExtractor:

    def __init__(self):
        self.dbConnector = database_utils.DatabaseConnector()
        self.engine = self.dbConnector.init_db_engine()
    
    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def read_rds_table(self, table):
        data = pd.read_sql_table(table, self.engine)
        return data
    
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, stream = True, pages = "all", lattice = True)
        df = pd.concat(dfs, ignore_index = True)
        return df


if __name__ == "__main__":
    ## Code testing the functionality 
    extractor = DataExtractor()

    df = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")