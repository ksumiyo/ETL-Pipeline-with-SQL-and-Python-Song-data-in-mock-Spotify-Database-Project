import pandas as pd
import json

df = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True)



            
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE If NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    song_id varchar, 
    artist_id varchar,
    session_id int, 
    location varchar, 
    user_agent varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar not null, 
    last_name varchar not null, 
    gender varchar, 
    level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL,
    artist_id varchar, 
    year int, 
    duration numeric NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL,
    location varchar, 
    latitude float, 
    longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int, 
    month int, 
    weekday varchar)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT 
    DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
    DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT 
    DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT
    DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, month, weekday)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs as s
JOIN artists as a
ON s.artist_id = a.artist_id
WHERE s.title = %s 
AND a.name = %s 
AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]