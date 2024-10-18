import database_utils
from sqlalchemy import inspect, text
import pandas as pd

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

if __name__ == "__main__":
    ## Code testing the functionality 
    extractor = DataExtractor()

    tables = extractor.list_db_tables()
    print(tables)

    print(extractor.read_rds_table("legacy_users"))