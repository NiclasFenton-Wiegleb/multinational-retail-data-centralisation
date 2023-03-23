import yaml
import sqlalchemy

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_db_creds(self, cred_file):
        with open(cred_file, 'r') as f:
            credentials = yaml.safe_load(f)
            return credentials
        
    def init_db_engine(self, credentials):
        db_host = credentials["RDS_HOST"]
        db_password = credentials["RDS_PASSWORD"]
        db_usr = credentials["RDS_USER"]
        db = credentials["RDS_DATABASE"]
        db_port = credentials["RDS_PORT"]
        db_engine = sqlalchemy.create_engine(url = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            db_usr, db_password, db_host, db_port, db
        ))
        return db_engine
    
    def list_db_tables(self, engine):
        data = sqlalchemy.metadata.reflect(engine)
        return data

    
extractor = DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
tables = extractor.list_db_tables(engine)


