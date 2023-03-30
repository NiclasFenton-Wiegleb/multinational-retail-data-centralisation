import pandas as pd

class DataCleaning:

    def __init__(self) -> None:
        pass

#FIX: Re-index dataframe after processing

    def clean_user_data(self, dataframe):
        #Assign index
        df = dataframe.set_index("index")

        #Drop rows with invalid data.
        null_df = df[df["first_name"] == "NULL"]
        df = df.drop(null_df.index)

        incorrect_data = df[df["country_code"].str.isalpha() == False]
        df = df.drop(incorrect_data.index)

        #Assign correct data type to columns.
        df[["first_name", "last_name", "company", "email_address", "address","phone_number", "user_uuid"]] =  df[["first_name", 
                                                                           "last_name", "company", "email_address", "address",
                                                                            "phone_number", "user_uuid"]].astype("string")
        df["country"] =  df["country"].astype("category")

        #Change columns to datatime.
        df["date_of_birth"] =  df["date_of_birth"].str.replace("-", "")
        df["join_date"] =  df["join_date"].str.replace("-", "")
        date_format = "%Y%m%d"
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], format= date_format, errors= "coerce")
        df["join_date"] = pd.to_datetime(df["join_date"], format= date_format, errors= "coerce")

        #Replace line space from address column.
        df["address"] = df["address"].str.replace("\n", ", ", regex= True)

        #Replace incorrect country codes and change type to category.
        df["country_code"] = df["country_code"].str.replace("GGB", "GB")
        df["country_code"] =  df["country_code"].astype("category")

        #Clean phone numbers.
        r1 = "[^0-9]+"
        df["phone_number"] = df["phone_number"].str.replace(r1, "")
        df["phone_number"] = df["phone_number"].str[-10:]

        #Adding country extensions to the front of phone numbers
        df["phone_number"] = df["country_code"].map({
            "DE": "0049",
            "GB": "0044",
            "US": "001"
        }).astype(str) + df["phone_number"]

        #Reset Index to be coherent
        df.reset_index(drop=True)

        return df
    
