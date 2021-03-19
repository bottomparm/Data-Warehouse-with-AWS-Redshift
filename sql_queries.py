import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
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

staging_events_table_create= ("""
  CREATE TABLE "staging_events" (
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
    "registration" FLOAT NOT NULL,
    "sessionId" INT NOT NULL,
    "song" VARCHAR,
    "status" INT NOT NULL,
    "ts" INT NOT NULL,
    "userAgent" VARCHAR NOT NULL,
    "userId" INT NOT NULL
  )
""")

staging_songs_table_create = ("""
  CREATE TABLE "staging_songs" (
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
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]