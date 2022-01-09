""" 
Sparkify ETL process to load JSON files in S3 into Redshift

Note: run create_tables.py before running this script
"""

# import libraries
import configparser
import psycopg2

# import SQL queries from sql_queries.py
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data into staging tables from S3 source

    Args:
        cur (psycopg2.extensions.cursor): Redshift DWH cursor
        conn (psycopg2.extensions.connection): Redshift DWH connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data into analytics tables from staging tables

    Args:
        cur (psycopg2.extensions.cursor): Redshift DWH cursor
        conn (psycopg2.extensions.connection): Redshift DWH connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Sparkify ETL to load data for analytics

    1. Load song and log data from JSON files on S3 into staging
    2. Load staging data into analytics table in DWH
    """

    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
