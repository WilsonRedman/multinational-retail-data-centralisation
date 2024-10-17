import yaml
from sqlalchemy import create_engine

class DatabaseConnector:

    def read_db_creds(self):

        with open("db_creds.yaml", "r") as stream:
            creds = yaml.safe_load(stream)
            return creds

    def init_db_engine(self):
        
        creds = self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://"
                               f"{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@"
                               f"{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}")
        
        return engine

