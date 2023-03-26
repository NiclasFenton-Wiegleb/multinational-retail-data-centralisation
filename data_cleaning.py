import data_extraction
import yaml
import sqlalchemy
import pandas as pd

class DataCleaning:

    def __init__(self) -> None:
        pass

    #def clean_user_data():

extractor = data_extraction.DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
tables = extractor.list_db_tables(engine)
metadata_obj = sqlalchemy.MetaData()
metadata_obj.reflect(bind=engine)
legacy_users = extractor.read_rds_table("legacy_users")

print(type(legacy_users))