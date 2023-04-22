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

Now that the database is complete and the star-based schema set up, SQL queries can be used to answer various questions and enable the business to make data-driven decisions. See below the questions answered by the corresponding queries in the SQL_queries file:

Task 1: How many stores does the business have and in which countries?

'''

SELECT country_code AS country,
	COUNT(store_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY country;

'''

Task 2: Which locations currently have the most stores?

'''

SELECT locality,
	COUNT(store_code) AS total_no_stores
	FROM dim_store_details
	GROUP BY locality
	ORDER BY total_no_stores DESC;

'''

Task 3: Which months typically produce the average highest revenue?

'''

/* Average revenue per month each year. */

WITH sales_table AS(
	SELECT *
	FROM
		dim_date_times
	INNER JOIN
	orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
	INNER JOIN
	dim_products ON orders_table.product_code = dim_products.product_code)
		SELECT  AVG(sum_sales) AS avg_sales,
		month
		FROM(
				SELECT year, month, SUM(product_quantity*product_price) AS sum_sales
				FROM sales_table
				GROUP BY year, month
				ORDER BY year DESC
			) AS sum_per_month
		GROUP BY month
		ORDER BY avg_sales DESC;

/* Total revenue per month over all time. */

WITH sales_table AS(
	SELECT *
	FROM
		dim_date_times
	INNER JOIN
	orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
	INNER JOIN
	dim_products ON orders_table.product_code = dim_products.product_code)
		SELECT  SUM(sum_sales) AS total_sales,
		month
		FROM(
				SELECT year, month, SUM(product_quantity*product_price) AS sum_sales
				FROM sales_table
				GROUP BY year, month
				ORDER BY year DESC
			) AS sum_per_month
		GROUP BY month
		ORDER BY total_sales DESC;

'''

Task 4: How many sales are coming from online?

'''

/* Add column called 'location'. */
ALTER TABLE dim_store_details
	ADD location VARCHAR(11);

/* Update new column based on store_type in the dim_store_details table. */
UPDATE dim_store_details
	SET location = 'Offline'
	WHERE store_type != 'Web Portal';

/* Join dim_store_details and orders_table tables, select columns with 
appropriate aggregations and group by location. */
WITH store_orders AS (
	SELECT *
	FROM dim_store_details
	JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
)
		SELECT COUNT(date_uuid) AS number_of_sales,
		SUM(product_quantity) AS product_quantity_count,
			location
		FROM store_orders AS sales_product
		GROUP BY location
		ORDER BY number_of_sales DESC;

'''

Task 5: What percentage of sales come through each type of store?

'''

WITH store_orders_product AS (
	SELECT *
	FROM dim_store_details
	JOIN orders_table ON dim_store_details.store_code = orders_table.store_code
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
)
		SELECT store_type,
			SUM(product_quantity*product_price) AS total_sales,
			SUM(product_quantity*product_price) * 100.00 / 
			(SELECT SUM(product_quantity*product_price) FROM store_orders_product) AS percentage_total
		FROM store_orders_product
		GROUP BY store_type
		ORDER BY percentage_total DESC;

'''

Task 6: Which month in each year produced the highest cost of sales?

'''

WITH date_orders_product AS (
	SELECT *
	FROM orders_table
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
	JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
)
		SELECT
			SUM(product_quantity*product_price) AS total_sales,
			year,
			month
		FROM date_orders_product
		GROUP BY year, month
		ORDER BY total_sales DESC;

'''

Task 7: What is our staff headcount?

'''

SELECT SUM(staff_numbers) AS total_staff_numbers,
	country_code
	FROM dim_store_details
	GROUP BY country_code
	ORDER BY total_staff_numbers DESC;

'''

Task 8: Which German store type is selling the most?

'''

WITH store_orders_product AS (
	SELECT *
	FROM orders_table
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
	JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
)
		SELECT
			SUM(product_quantity*product_price) AS total_sales,
			store_type,
			country_code
		FROM store_orders_product
		WHERE country_code = 'DE'
		GROUP BY country_code, store_type
		ORDER BY total_sales ASC;

'''

Task 9: How quickly is the company making sales?

'''

/* Add actual_time_taken column to dim_date_times. */
ALTER TABLE dim_date_times
	ADD COLUMN actual_time_taken interval


/* Update the values in the actual_time_taken column by subtracting the current timestamp with the one from
the closest preceeding timestamp. */
UPDATE dim_date_times
	SET actual_time_taken = previous_sale.actual_time_taken
	FROM
		(
			SELECT timestamp - MIN(timestamp) OVER (ORDER BY timestamp ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
			AS actual_time_taken,
			timestamp,
			year,
			index
			FROM dim_date_times
			GROUP BY year, timestamp, index
		) AS previous_sale
	WHERE previous_sale.index = dim_date_times.index;

/* Select the average time between sales per year.*/
	
SELECT year,
	AVG(actual_time_taken) AS avg_actual_time_taken
FROM dim_date_times
GROUP BY year
ORDER BY avg_actual_time_taken DESC;

'''