import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_DB = config["CLUSTER"]["DB_NAME"]
DWH_DB_USER = config["CLUSTER"]["DB_USER"]
DWH_DB_PASSWORD = config["CLUSTER"]["DB_PASSWORD"]
DWH_HOST = config["ENDPOINT"]["host"]
DWH_PORT = config["ENDPOINT"]["port"]


def load_staging_tables(cur, conn):
    """Execute the staging table queries from sql_queries.py and commit the changes"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Execute the insert table queries from sql_queries.py and commit the changes"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to postgres"""
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT
        )
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
