import yaml
from flask_sqlalchemy import SQLAlchemy

class DataExtractor:
    def __init__(self) -> None:
        pass

    def read_db_creds(self, cred_file):
        with open(cred_file, 'r') as f:
            credentials = yaml.safe_load(f)
            return credentials
        
    def init_db_engine(self, credentials):
        db_engine = create_engine(credentials)
        return db_engine

    
extractor = DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)


