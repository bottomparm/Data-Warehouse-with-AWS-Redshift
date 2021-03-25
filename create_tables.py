import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_DB = config["CLUSTER"]["DB_NAME"]
DWH_DB_USER = config["CLUSTER"]["DB_USER"]
DWH_DB_PASSWORD = config["CLUSTER"]["DB_PASSWORD"]
DWH_HOST = config["ENDPOINT"]["host"]
DWH_PORT = config["ENDPOINT"]["port"]


def drop_tables(cur, conn):
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_tables(cur, conn):
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    # connect to postgres db
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT
        )
    )
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
