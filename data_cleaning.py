import data_extraction
import yaml
import sqlalchemy
import pandas as pd
from datetime import datetime

def try_convert_type(value, default, *types):
    for t in types:
        try:
            return t(value)
        except (ValueError, TypeError):
            return default

class DataCleaning:

    def __init__(self) -> None:
        pass

    def clean_store_data(self, dataframe):
        df = dataframe.set_index("index")
        df = df.reindex(columns= ["address", "longitude", "lat", "latitude", "locality", 
                                  "country_code", "continent", "store_code", "store_type", 
                                  "store_type", "staff_numbers", "opening_date"])
        remove_non_numerics = lambda val : try_convert_type(val, None, float)
        df = df.drop(df.index.get_loc(df.loc[df["lat"].notnull()]))
        df["address"] = df["address"].astype("string")
        df["longitude"] = df["longitude"].apply(remove_non_numerics)
        df["latitude"] = df["latitude"].apply(remove_non_numerics)
        df["address"] = df["address"].str.replace('\n', ', ', regex= True)
        return df
    
    def clean_user_data(self, dataframe):
        df = dataframe.set_index("index")
        df[["first_name", "last_name", "company", "email_address"]] =  df[["first_name", 
                                                                           "last_name", "company", 
                                                                           "email_address"]].astype("string")
        df[["country", "country_code"]] =  df[["country", "country_code"]].astype("category")
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
clean_user_data = cleaner.clean_user_data(legacy_users)

print(clean_user_data[["date_of_birth", "join_date"]])
print(clean_user_data.info())

