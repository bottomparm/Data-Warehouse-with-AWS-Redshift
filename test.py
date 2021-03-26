import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_HOST = config["ENDPOINT"]["host"]
DWH_DB = config["CLUSTER"]["DB_NAME"]
DWH_DB_USER = config["CLUSTER"]["DB_USER"]
DWH_DB_PASSWORD = config["CLUSTER"]["DB_PASSWORD"]
DWH_PORT = config["ENDPOINT"]["port"]

def testQueries(cur, conn):
  query1 = """
  SELECT songplays.song_id, songs.title, artists.artist_name as artist, users.first_name, time.hour
  FROM songplays
  JOIN songs ON (songplays.song_id = songs.song_id)
  JOIN artists ON (songplays.artist_id = artists.artist_id)
  JOIN users ON (songplays.user_id = users.user_id)
  JOIN time ON (songplays.start_time = time.start_time)
  WHERE songs.year = 1999
  LIMIT 30;
  """
  cur.execute(query1)
  my_table = pd.read_sql(query1, conn)
  print(my_table)

  query2 = """
  SELECT time.start_time, time.hour, time.day, time.week, time.month, time.year, time.weekday
  FROM time
  WHERE day = 4
  LIMIT 30;
  """
  cur.execute(query2)
  my_table1 = pd.read_sql(query2, conn)
  print(my_table1)


def main():
    """Connect to postgres"""
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT
        )
    )
    cur = conn.cursor()
    testQueries(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()