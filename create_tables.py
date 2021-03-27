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

def create_schemas(cur, conn):
    '''
    Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in create_schemas_queries:
        cur.execute(query)
        conn.commit()        

def drop_schemas(cur, conn):
    '''
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()

def drop_tables(cur, conn):
    """Drop tables if they exist and commit the changes"""
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def create_tables(cur, conn):
    """Create tables if they do not exist and commit the changes"""
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        print(e)


def main():
    """Connect to postgres"""
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT
        )
    )
    cur = conn.cursor()

    create_schemas(cur, conn)
    drop_schemas(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
