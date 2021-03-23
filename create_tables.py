import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import awsConfig

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_IAM_ROLE_NAME = config["IAM_ROLE"]["NAME"]
DWH_DB = config["CLUSTER"]["DB_NAME"]
DWH_DB_USER = config["CLUSTER"]["DB_USER"]
DWH_DB_PASSWORD = config["CLUSTER"]["DB_PASSWORD"]
DWH_CLUSTER_IDENTIFIER = config["CLUSTER"]["CLUSTER_IDENTIFIER"]
DWH_CLUSTER_TYPE = config["CLUSTER"]["CLUSTER_TYPE"]
DWH_NUM_NODES = config["CLUSTER"]["NUM_NODES"]
DWH_NODE_TYPE = config["CLUSTER"]["NODE_TYPE"]
DWH_PORT = config["CLUSTER"]["DB_PORT"]

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
    endpoint = awsConfig.main()
    host = endpoint['Address']
    port = endpoint['Port']

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            host, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, port
        )
    )
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
