import data_extraction
import yaml
import sqlalchemy
import pandas as pd

class DataCleaning:

    def __init__(self) -> None:
        pass

    def clean_user_data(self, dataframe):
        df = dataframe.set_index("index")
        df["address"] = df["address"].astype("string")
        df["address"] = df["address"].str.replace('\n', ', ', regex= True)
        return df

extractor = data_extraction.DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
tables = extractor.list_db_tables(engine)
metadata_obj = sqlalchemy.MetaData()
metadata_obj.reflect(bind=engine)
legacy_store_details = extractor.read_rds_table("legacy_store_details")
legacy_users = extractor.read_rds_table("legacy_users")
orders_table = extractor.read_rds_table("orders_table")

#cleaned data:

cleaner = DataCleaning()
clean_legacy_store_details = cleaner.clean_user_data(legacy_store_details)

print(clean_legacy_store_details)
print(clean_legacy_store_details.info())