import pandas as pd

class DataCleaning:

    def __init__(self) -> None:
        pass


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

        #Reset Index
        df.reset_index(drop=True)

        return df
    
    def clean_card_data(self, dataframe):

        #Drop incorrect format in card_number column
        dataframe["card_number"] = dataframe["card_number"].str[-16:]
        incorrect_data = dataframe[dataframe["card_number"].str.isnumeric() == False]
        df = dataframe.drop(incorrect_data.index)

        #Drop missing values
        df = df.dropna()

        #Assign datatypes to card_number columns
        df["card_number"] =  df["card_number"].astype("int")


        #Change columns to datatime.
        df["expiry_date"] =  df["expiry_date"].str.replace("/", "")
        df["date_payment_confirmed"] =  df["date_payment_confirmed"].str.replace("-", "")

        expiry_date_format = "%m%y"
        payment_date_format = "%Y%m%d"

        df["expiry_date"] = pd.to_datetime(df["expiry_date"], format= expiry_date_format, errors= "coerce")
        df["date_payment_confirmed"] = pd.to_datetime(df["date_payment_confirmed"], format= payment_date_format,
                                                    errors= "coerce")

        #Make card_provider column more uniform
        df["card_provider"] =  df["card_provider"].str.replace(" 16 digit", "")
        df["card_provider"] =  df["card_provider"].str.replace(" 15 digit", "")
        df["card_provider"] =  df["card_provider"].str.replace(" 19 digit", "")
        df["card_provider"] =  df["card_provider"].str.replace(" 13 digit", "")

        #Assign str datatype to card_provider column
        df["card_provider"] =  df["card_provider"].astype("category")

        return df
    
    def clean_store_data(self, dataframe):
        df = dataframe

        #Set index column as index
        df = df.set_index("index")

        #Drop invalid and empty rows
        incorrect_data = df[(df["lat"].map(type) == str) == True]
        df = df.drop(incorrect_data.index)

        null_df = df[df["staff_numbers"].isnull()]
        df = df.drop(null_df.index)

        #Drop empty column
        df = df.drop("lat", axis= 1)

        #Change column order to be more intuitive
        df = df.loc[:, ["address", "locality", "country_code", "longitude", "latitude",  "continent", "store_type",
                "store_code", "staff_numbers", "opening_date" ]]

        #Replace line space from address column.
        df["address"] = df["address"].str.replace("\n", ", ", regex= True)

        #Clean staff_numbers
        r1 = "[^0-9]+"
        df["staff_numbers"] = df["staff_numbers"].str.replace(r1, "")

        #Clean continent column
        df["continent"] = df["continent"].str.replace("ee", "")

        #Change columns to datatime.
        df["opening_date"] =  df["opening_date"].str.replace("-", "")

        opening_date_format = "%Y%m%d"
        df["opening_date"] = pd.to_datetime(df["opening_date"], format= opening_date_format, errors= "coerce")

        #Assign types to columns
        df["staff_numbers"] = df["staff_numbers"].astype(int)
        df[["longitude", "latitude"]] = df[["longitude", "latitude"]].astype(float)
        df[["country_code", "continent", "store_type"]] = df[["country_code", "continent", "store_type"]].astype("category")

        return df
    

#Clean products data

#Load file and assign index
df = pd.read_csv("products.csv")
df.rename(columns= {"Unnamed: 0": "index"}, inplace= True)
df = df.set_index("index")

#Identify and drop incorrect data entries
incorrect_data = pd.read_csv("products.csv")
incorrect_data["product_price"] = incorrect_data["product_price"].str.strip("£")
incorrect_data["product_price"] = incorrect_data["product_price"].str.replace(".", "")
incorrect_data = incorrect_data[incorrect_data["product_price"].str.isnumeric() == False]
df = df.drop(incorrect_data.index)

#Assign product_price as float type
df["product_price"] = df["product_price"].str.strip("£")
df["product_price"] = df["product_price"].astype(float)

#Convert all weights to kg in weight column

print(df["product_price"].unique())
print(df.info())