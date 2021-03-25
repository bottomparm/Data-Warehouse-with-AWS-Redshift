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
    "artist_name" TEXT,
    "auth" TEXT,
    "first_name" TEXT,
    "gender" CHAR,
    "itemInSession" INTEGER,
    "last_name" TEXT,
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
    user_id INTEGER NOT NULL,
    level TEXT,
    song_id TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    location TEXT,
    user_agent TEXT
  )
"""

user_table_create = """
  CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
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
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {}
    ;
"""
).format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSON_PATH)

staging_songs_copy = (
    """
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    JSON 'auto'
    ;
"""
).format(S3_SONG_DATA, DWH_IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (user_id, level, session_id, location, user_agent, song_id, artist_id, start_time)
    SELECT userid, level, sessionid, location, useragent, song_id, artist_id, CAST(ts as date)
    FROM staging_events
    JOIN staging_songs ON (staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name);
  """

user_table_insert = """
  INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT userid, firstname, lastname, gender, level
  FROM staging_events
  ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
  """

song_table_insert = """
  INSERT INTO songs (song_id, title, year, duration, artist_id)
  SELECT song_id, title, year, CAST(duration as float), artist_id 
  FROM staging_songs;
  """

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
ON CONFLICT (artist_id) DO NOTHING;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT CAST(ts as date), EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
FROM staging_events
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
