import data_extraction
import yaml
import sqlalchemy
import pandas as pd

def tryconvert(value, default, *types):
    for t in types:
        try:
            return t(value)
        except (ValueError, TypeError):
            continue
    return default

class DataCleaning:

    def __init__(self) -> None:
        pass

    def clean_user_data(self, dataframe):
        df = dataframe.set_index("index")
        df = df.reindex(columns= ["address", "longitude", "lat", "latitude", "locality", 
                                  "country_code", "continent", "store_code", "store_type", 
                                  "store_type", "staff_numbers", "opening_date"])
        remove_non_numerics = lambda val : tryconvert(val, None, float)
        df["address"] = df["address"].astype("string")
        df["longitude"] = df["longitude"].apply(remove_non_numerics)
        df["latitude"] = df["latitude"].apply(remove_non_numerics)
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
print(clean_legacy_store_details["localitiy"])

# "address", "longitude", "lat", "latitude", "locality", "country_code", "continent", "store_code", 
# "store_type", "store_type", "staff_numbers", "opening_date"