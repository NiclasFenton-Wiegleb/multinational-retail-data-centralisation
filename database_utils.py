import data_cleaning
import data_extraction
import sqlalchemy
import psycopg2
import config
import pandas as pd

class DatabaseConnector:

    def __init__(self) -> None:
        pass

    def upload_to_db(self, dataframe, table_name):

        password = config.password

        #Connect to database
        db_engine = sqlalchemy.create_engine(url = f"postgresql://postgres:{password}@localhost/Sales_Data")

        #Upload dataframe to database
        conn = psycopg2.connect(
            database= "Sales_Data",
            user= "postgres",
            password= f"{password}",
            host= "localhost",
            port= "5432"
        )

        conn.autocommit = True
        cursor = conn.cursor()

        #Drop table if already exists
        sql = f"DROP TABLE IF EXISTS {table_name}"

        cursor.execute(sql)

        sql1 = f'''CREATE TABLE {table_name} (index int, first_name char(20), last_name char(20), date_of_birth date, company char(100), 
            email_address char(70), address char(100), country char(20), country_code char(2), phone_number char(20),
            join_date date, user_uuid char(100));'''

        cursor.execute(sql1)

        #Convert dataframe to sql
        dataframe.to_sql(table_name, con=db_engine, if_exists='replace')

        #Fetching all rows
        sql2 = f"SELECT * from {table_name}"
        cursor.execute(sql2)
        for i in cursor.fetchall():
            print(i)
        cursor.close()
        del cursor
        

#Extract data

extractor = data_extraction.DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine()
#card_details = extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")

#cleaned data:

cleaner = data_cleaning.DataCleaning()

#Load data
df = pd.read_csv("pdf_to_csv.csv")

clean_cards = cleaner.clean_card_data(df)


#Upload products data

uploader = DatabaseConnector()
data_upload = uploader.upload_to_db(clean_cards, "dim_card_details")



