""" 
Sparkify ETL process to load JSON files into AWS Redshift

Note: run create_tables.py before runnign this script
"""

# import libraries
import configparser
import psycopg2

# import SQL queries from sql_queries.py
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Sparkify ETL to load data

    1. Load song data from JSON files on S3
    2. Load log data from JSON files on S3
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
