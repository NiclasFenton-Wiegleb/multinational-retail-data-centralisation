import data_cleaning
import data_extraction
import pandas as pd
import sqlalchemy
import psycopg2

class DatabaseConnector:

    def __init__(self) -> None:
        pass

    def upload_to_db(self, dataframe, table_name):

        #Connect to database
        db_engine = sqlalchemy.create_engine(url = "postgresql://postgres:pass@localhost/Sales_Data")
        db_connection = db_engine.connect()

        #Upload dataframe to database
        conn = psycopg2.connect(
            database= "Sales_Data",
            user= "postgres",
            password= "pass",
            host= "localhost",
            port= "5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        #Drop table if already exists
        cursor.execute("drop table if exists user_data")

        sql = '''CREATE TABLE user_data(index int, first_name char(20), last_name char(20), date_of_birth date, company char(100), 
        email_address char(70), address char(100), country char(20), country_code char(2), phone_number char(20), join_date date, user_uuid char(100));'''

        cursor.execute(sql)

        #Convert dataframe to sql
        df = dataframe
        df.to_sql("user_data", con=db_engine, if_exists='replace')
        cursor.execute("SELECT * FROM user_data").fetchall()

        #Fetching all rows
        #sql1 = '''SELECT * from user_data;'''
       # cursor.execute(sql1)
        #for i in cursor.fetchall():
            #print(i)

        conn.commit()
        conn.close()

#Extract data

extractor = data_extraction.DataExtractor()
file = 'db_creds.yaml'
cred = extractor.read_db_creds(file)
engine = extractor.init_db_engine(cred)
legacy_users = extractor.read_rds_table("legacy_users")


#cleaned data:

cleaner = data_cleaning.DataCleaning()
clean_user_data = cleaner.clean_user_data(legacy_users)


#Upload clean_user_data

uploader = DatabaseConnector()
data_upload = uploader.upload_to_db(clean_user_data, "user_data")

