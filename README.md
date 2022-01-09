# Sparkify Data Warehouse

## Motivation

The startup, Sparkify, is a music streaming app and wants to analyse what songs their users are listening to. Their current data is available as local JSON files on AWS S3 and cannot be easily analised. The purpose of the ETL process is to load the JSON files from S3 into Redshift and load this data into the analytics data warehouse.

## Data model

Data was normalized into a star schema with fact and dimention tables in order to use data for the analysis but also reuse data for future analysis.

## File descriptions

- `create_tables.py`: Connects to Redshift database, drops existing tables and creates the required staging and analytics tables.
- `sql_queries.py`: Contains all the SQL queries used to drop, create and insert data into Redshift.
- `etl.py`: Load JSON files (song and log files) from S3 into staging tables and insert staged data into the data model for analytics.
- `test.ipynb`: Tests to count rows in the analytics table and display top 3 rows in each table.

**NOTE: Need to run `create_tables.py` before running `etl.py`.**

## How to run the files and notebooks

Dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

## License

GNU GPL v3

## Author

Coenraad Pretorius
