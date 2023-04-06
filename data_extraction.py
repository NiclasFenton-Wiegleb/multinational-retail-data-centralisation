import yaml
import sqlalchemy
import pandas as pd
import tabula
import requests
import boto3


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
            self.host = credentials["RDS_HOST"]
            self.password = credentials["RDS_PASSWORD"]
            self.user = credentials["RDS_USER"]
            self.database = credentials["RDS_DATABASE"]
            self.port = credentials["RDS_PORT"]
            return credentials
        
    def init_db_engine(self):
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
    
    def retrieve_pdf_data(self, pdf_path):
        tabula.convert_into(pdf_path, "pdf_to_csv.csv", output_format= "csv", pages= "all")
        df = pd.read_csv("pdf_to_csv.csv")
        return df
    
    def list_number_of_stores(self, api, auth_details):
        request = requests.get(api, headers= auth_details)
        data = request.json()
        return data["number_stores"]
    
    def retrieve_store_data(self, api, auth_details):

        #Get the number of stores
        number_stores = self.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", auth_details)

        #Request data for each store and append to 
        #store_data as list of dictionaries
        store_data = []
        for store_number in range(number_stores):
            request = requests.get(api + f"{store_number}", headers= auth_details)
            data = request.json()
            store_data.append(data)
            
        #Convert list of dictionaries to dataframe and select index
        df = pd.DataFrame(store_data)
        df = df.set_index("index")

        return df
    
    def extract_from_s3(self, s3_address):
        #Split s3_address into elements
        string = s3_address.split("/")
        bucket = string[2]
        file = string[3]

        #Initiate s3
        s3 = boto3.client("s3", "us-east-1")

        #Download file from s3 bucket
        s3.download_file(bucket, file, file)

        #Convert csv file to dataframe
        df = pd.read_csv(file)

        return df

#Extract data

extractor = DataExtractor()
#file = 'db_creds.yaml'
#cred = extractor.read_db_creds(file)
#engine = extractor.init_db_engine()
#card_details = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

df = extractor.retrieve_pdf_data(link)

#df.to_csv("products.csv")