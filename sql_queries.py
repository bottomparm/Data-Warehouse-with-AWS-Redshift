import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_IAM_ROLE_ARN = config["IAM_ROLE"]["rolearn"]
S3_LOG_DATA = config["S3"]["log_data"]
S3_SONG_DATA = config["S3"]["song_data"]
S3_LOG_JSON_PATH = config["S3"]["log_jsonpath"]

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# SONG DATASET SAMPLE
# {
#   "num_songs": 1,
#   "artist_id": "ARJIE2Y1187B994AB7",
#   "artist_latitude": null,
#   "artist_longitude": null,
#   "artist_location": "",
#   "artist_name": "Line Renaud",
#   "song_id": "SOUPIRU12A6D4FA1E1",
#   "title": "Der Kleine Dompfaff",
#   "duration": 152.92036,
#   "year": 0}

# CREATE TABLES

staging_events_table_create = """
  CREATE TABLE IF NOT EXISTS staging_events (
    "artist_name" VARCHAR NOT NULL,
    "auth" VARCHAR NOT NULL,
    "first_name" VARCHAR NOT NULL,
    "gender" VARCHAR NOT NULL,
    "itemInSession" INT NOT NULL,
    "last_name" VARCHAR NOT NULL,
    "length" NUMERIC(10,5),
    "level" VARCHAR NOT NULL,
    "location" VARCHAR NOT NULL,
    "method" VARCHAR NOT NULL,
    "page" VARCHAR NOT NULL,
    "registration" BIGINT NOT NULL,
    "sessionId" INT NOT NULL,
    "song" VARCHAR,
    "status" INT NOT NULL,
    "ts" BIGINT NOT NULL,
    "userAgent" VARCHAR NOT NULL,
    "userId" INT NOT NULL
  )
"""

staging_songs_table_create = """
  CREATE TABLE IF NOT EXISTS staging_songs (
    "num_songs" INT NOT NULL,
    "artist_id" VARCHAR NOT NULL,
    "artist_latitude" NUMERIC(10,5),
    "artist_longitude" NUMERIC(10,5),
    "artist_location" VARCHAR,
    "artist_name" VARCHAR NOT NULL,
    "song_id" VARCHAR NOT NULL,
    "title" VARCHAR NOT NULL,
    "duration" NUMERIC(10,5) NOT NULL,
    "year" INT NOT NULL
  )
"""

songplay_table_create = """
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT NOT NULL,
    location VARCHAR NOT NULL,
    user_agent VARCHAR NOT NULL
  )
"""

user_table_create = """
  CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
  )
"""

song_table_create = """
  CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY NOT NULL,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration NUMERIC(10,5) NOT NULL,
    UNIQUE (song_id)
  )
"""

artist_table_create = """
  CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY NOT NULL,
    artist_latitude NUMERIC(10,5),
    artist_longitude NUMERIC(10,5),
    artist_location VARCHAR,
    artist_name VARCHAR NOT NULL,
    UNIQUE (artist_id)
  )
"""

time_table_create = """
  CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY NOT NULL,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
  )
"""

# STAGING TABLES

staging_events_copy = (
"""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    delimiter ','
    ;
"""
).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN)

staging_songs_copy = (
"""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    delimiter ','
    ;
"""
).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
"""

song_table_insert = """
    INSERT INTO songs (artist_id, song_id, title, duration, year)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
"""

artist_table_insert = """
    INSERT INTO artists (artist_id, artist_latitude, artist_longitude, artist_location, artist_name)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING 
"""

time_table_insert = """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
