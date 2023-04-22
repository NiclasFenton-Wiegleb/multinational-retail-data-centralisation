# Multinational Data Centralisation

This project simulates the scenario of working for a multinational corporation that sells various goods across the globe. Their sales data is spread across several different data sources and is difficult to access or analyse. The project seeks to extract the data from its various sources, clean it and store it in a centralised location that enables the company to make data-driven decisions.

Technologies used:

- Python
- YAML
- Pandas
- AWS database
- API
- s3 buckets
- pgAdmin4
- PostgreSQL

Dependencies:

- sqlalchemy
- pandas
- yaml
- tabula
- requests
- boto3

## Milestone 1 - Extract and Clean Sales Data

The various datasets are stored in various formats and locations. Therefore, a pipline is required to extract the data, clean it and format it to be consistent and usable once it's uploaded to a database created in pgAdmin4. See some of the formats and data sources below:

- AWS RDS database
- PDF in AWS S3 bucket
- JSON stored on server

The pipline established to achieve this task consists of data_extraction.py, data_cleaning.py and database_utils.py. The first has methods to retrieve the data from the various locations it is stored and pass it to the methods contained in the second script. As each extracted dataset requires different forms of cleaning, data_cleaning.py contains a method for each dataset and converts the raw data into a usable format. Finally, database_utils.py uploads the clean data to the database on a local server and accessible via pgAdmin4.

## Milestone 2 - Create DataBase Schema

With the data uploaded, some adjustments need to be made to ensure columns are assigned the correct data type etc. and some additional columns are needed (e.g. to categorise the weight range in the "products" table as light, mid-sized, heavy or requiring a truck to ship). For each table the primary key needs to be chosen based on which columns contain unique identifiers and are also present in the "orders" table.

Once the primary keys have been assigned, a star-based schema is established for the database by assigning foreign key constraints to the appropriate columns in the "orders" table to link them to the primary keys set in the other tables.

## Milestone 3 - Query Data