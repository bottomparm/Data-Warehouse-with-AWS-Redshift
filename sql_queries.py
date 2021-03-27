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

# SCHEMA CREATE/DELETE

# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")

# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")

# CREATE TABLES

# having issues with the ts BIGINT type and getting it into timestamp form
staging_events_table_create = """
  CREATE TABLE IF NOT EXISTS staging_events (
    "artist" TEXT,
    "auth" TEXT,
    "firstName" TEXT,
    "gender" CHAR,
    "itemInSession" INTEGER,
    "lastName" TEXT,
    "length" DOUBLE PRECISION,
    "level" TEXT,
    "location" TEXT,
    "method" TEXT,
    "page" TEXT,
    "registration" DOUBLE PRECISION,
    "sessionId" INTEGER,
    "song" TEXT,
    "status" INTEGER,
    "ts" BIGINT,
    "userAgent" TEXT,
    "userId" INTEGER
  )
"""

staging_songs_table_create = """
  CREATE TABLE IF NOT EXISTS staging_songs (
    "num_songs" INTEGER,
    "artist_id" TEXT,
    "artist_latitude" DOUBLE PRECISION,
    "artist_longitude" DOUBLE PRECISION,
    "artist_location" TEXT,
    "artist_name" TEXT,
    "song_id" TEXT,
    "title" TEXT,
    "duration" DOUBLE PRECISION,
    "year" INTEGER
  )
"""

songplay_table_create = """
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level TEXT,
    song_id TEXT,
    artist_id TEXT,
    session_id INTEGER,
    location TEXT,
    user_agent TEXT
  )
"""

# had to remove PRIMARY KEY from user_id to get rid of "cannot insert NULL value for user_id" error
user_table_create = """
  CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
  )
"""

song_table_create = """
  CREATE TABLE IF NOT EXISTS songs (
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    UNIQUE (song_id)
  )
"""

artist_table_create = """
  CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location TEXT,
    artist_name TEXT NOT NULL,
    UNIQUE (artist_id)
  )
"""

time_table_create = """
  CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
  )
"""

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS json {}
    ;
"""
).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSON_PATH)

staging_songs_copy = (
    """
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    JSON 'auto'
    ;
"""
).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FACT TABLE

songplay_table_insert = """
  INSERT INTO songplays (user_id, level, session_id, location, user_agent, song_id, artist_id, start_time)
  SELECT userid, level, sessionid, location, useragent, song_id, artist_id, DATEADD(ms, ts, '1970-01-01 00:00:00')
  FROM staging_events
  JOIN staging_songs ON (staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name)
"""

# DIMENSION TABLES

user_table_insert = """
  INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT userid, firstname, lastname, gender, level
  FROM staging_events
"""

song_table_insert = """
  INSERT INTO songs (song_id, title, year, duration, artist_id)
  SELECT song_id, title, year, CAST(duration as float), artist_id 
  FROM staging_songs
"""

artist_table_insert = """
  INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
  SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs
"""

time_table_insert = """
  INSERT INTO time (start_time, hour, day, week, month, year, weekday)
  SELECT 
    DATEADD(ms, ts, '1970-01-01 00:00:00'),
    EXTRACT(hour from DATEADD(ms, ts, '1970-01-01 00:00:00')),
    EXTRACT(day from DATEADD(ms, ts, '1970-01-01 00:00:00')),
    EXTRACT(week from DATEADD(ms, ts, '1970-01-01 00:00:00')),
    EXTRACT(month from DATEADD(ms, ts, '1970-01-01 00:00:00')),
    EXTRACT(year from DATEADD(ms, ts, '1970-01-01 00:00:00')),
    EXTRACT(dayofweek from DATEADD(ms, ts, '1970-01-01 00:00:00'))
  FROM staging_events
"""

# QUERY LISTS

create_schema_queries = [
    fact_schema,
    dimension_schema,
    staging_schema
]
drop_schema_queries = [
    fact_schema_drop,
    dimension_schema_drop,
    staging_schema_drop
]
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
