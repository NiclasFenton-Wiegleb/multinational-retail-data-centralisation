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
    
# TO FIX: Phone numbers need to be uniform format;
# errors with dates; drop rows with wrong info; check for duplicates


    def clean_user_data(self, dataframe):
        df = dataframe.set_index("index")
        null_df = df[df["first_name"] == "NULL"]
        df = df.drop(null_df.index)
        incorrect_data = df[df["country_code"].str.isalpha() == False]
        df = df.drop(incorrect_data.index)
        df[["first_name", "last_name", "company", "email_address", "address", "user_uuid"]] =  df[["first_name", 
                                                                           "last_name", "company", 
                                                                           "email_address", "address", "user_uuid"]].astype("string")
        df[["country", "country_code"]] =  df[["country", "country_code"]].astype("category")
        df["date_of_birth"] =  df["date_of_birth"].str.replace("-", "")
        df["join_date"] =  df["join_date"].str.replace("-", "")
        date_format = "%Y%m%d"
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], format= date_format, errors= "coerce")
        df["join_date"] = pd.to_datetime(df["join_date"], format= date_format, errors= "coerce")
        return df

extractor = data_extraction.DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
#tables = extractor.list_db_tables(engine)
#metadata_obj = sqlalchemy.MetaData()
#metadata_obj.reflect(bind=engine)
#legacy_store_details = extractor.read_rds_table("legacy_store_details")
legacy_users = extractor.read_rds_table("legacy_users")
#orders_table = extractor.read_rds_table("orders_table")

#cleaned data:

cleaner = DataCleaning()
clean_user_data = cleaner.clean_user_data(legacy_users)
x = clean_user_data[clean_user_data.isnull().any(axis=1)]
y = clean_user_data[clean_user_data["country_code"].str.isalpha() == False]

print(x)
#print(clean_user_data)
#print(clean_user_data.describe())

