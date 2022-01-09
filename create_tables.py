"""
Sparkify Data ETL for AWS Redshift

This file will drop and create the required staging and analytics tables.
"""

# import libraries
import configparser
import psycopg2

# load SQL queries from sql_queries.py
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Load config and connect to Redshift.
    - Drop tables if they exists.
    - Create all tables needed.
    - Finally, closes the connection.
    """

    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config["CLUSTER"].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
