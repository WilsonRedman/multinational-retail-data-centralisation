import yaml
from sqlalchemy import create_engine
import pandas as pd

class DatabaseConnector:

    def read_db_creds(self):

        with open("db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
            return creds
        
    def read_local_creds(self):

        with open("local_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
            return creds

    def init_db_engine(self):
        
        creds = self.read_db_creds()

        engine = create_engine("postgresql+psycopg2://"
                               f"{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@"
                               f"{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}")
        
        return engine
    
    def upload_to_db(self, data, table):

        creds = self.read_local_creds()

        engine = create_engine("postgresql+psycopg2://"
                                f"{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@"
                                f"{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}")
        
        data.to_sql(table, engine, if_exists = "replace")
