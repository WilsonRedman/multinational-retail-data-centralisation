from sqlalchemy import create_engine
import yaml

class DatabaseConnector:

    def __read_db_creds(self, file):
        '''
        Takes a yaml file containing database creds and converts to ditionary

        Args:
            file (string): File name containing the credentials

        Returns:
            dictionary: Containing the credentials
        '''
        with open(file, "r") as stream:
            creds = yaml.safe_load(stream)
            return creds

    def init_db_engine(self):
        '''
        Initialises a SqlAlchemy database engine based on credentials stored in "db_creds.yaml"

        Returns:
            SqlAlchemy database engine
        '''
        creds = self.__read_db_creds("db_creds.yaml")

        engine = create_engine("postgresql+psycopg2://"
                               f"{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@"
                               f"{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}")
        
        return engine
    
    def upload_to_db(self, data, table):
        '''
        Takes data and a table name, uploading the data to a local database initialised using
        credentials stored in "local_creds.yaml"

        Args:
            data (DataFrame): Containing the data to store
            table (String): Name of the table to store in
        '''

        creds = self.__read_db_creds("local_creds.yaml")

        engine = create_engine("postgresql+psycopg2://"
                                f"{creds["RDS_USER"]}:{creds["RDS_PASSWORD"]}@"
                                f"{creds["RDS_HOST"]}:{creds["RDS_PORT"]}/{creds["RDS_DATABASE"]}", echo=True)
        
        data.to_sql(table, engine, if_exists = "replace")
