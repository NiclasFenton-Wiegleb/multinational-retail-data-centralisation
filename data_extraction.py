import yaml
import sqlalchemy
import pandas as pd


class DataExtractor:
    def __init__(self):
        host:None
        password:None
        user: None
        database: None
        port: None
        

    def read_db_creds(self, cred_file):
        with open(cred_file, 'r') as f:
            credentials = yaml.safe_load(f)
            return credentials
        
    def init_db_engine(self, credentials):
        self.host = credentials["RDS_HOST"]
        self.password = credentials["RDS_PASSWORD"]
        self.user = credentials["RDS_USER"]
        self.database = credentials["RDS_DATABASE"]
        self.port = credentials["RDS_PORT"]
        db_engine = sqlalchemy.create_engine(url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            self.user, self.password, self.host, self.port, self.database
        ))
        return db_engine
    
    def list_db_tables(self, engine):
        metadata_obj = sqlalchemy.MetaData()
        metadata_obj.reflect(bind=engine)
        list_tables = list(metadata_obj.tables.keys())
        return list_tables
    
    def read_rds_table(self, table_name):
        connection = sqlalchemy.create_engine(url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            self.user, self.password, self.host, self.port, self.database
        )).connect()
        df = pd.read_sql_table(table_name, connection)
        return df



    
extractor = DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
tables = extractor.list_db_tables(engine)
metadata_obj = sqlalchemy.MetaData()
metadata_obj.reflect(bind=engine)
legacy_users = extractor.read_rds_table("legacy_users")

print(type(legacy_users))

