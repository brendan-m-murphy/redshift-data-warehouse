from src import utils


# CONFIG
config = utils.get_s3_config()
LOG_DATA = config['LOG_DATA']
LOG_JSONPATH = config['LOG_JSONPATH']
SONG_DATA = config['SONG_DATA']

_, IAM_ARN = utils.get_role_name_arn()

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE event_staging (
id BIGINT IDENTITY(0, 1),
artist TEXT,
auth TEXT,
firstName TEXT,
gender TEXT,
itemInSession INT,
lastName TEXT,
length DECIMAL,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration DECIMAL,
sessionId TEXT,
song TEXT,
status TEXT,
ts TIMESTAMP,
userAgent TEXT,
userId TEXT);
""")

staging_songs_table_create = ("""
CREATE TABLE song_staging (
id BIGINT IDENTITY(0, 1),
song_id TEXT,
num_songs INT,
title TEXT,
artist_name TEXT,
artist_latitude DECIMAL,
year INTEGER,
duration DECIMAL,
artist_id TEXT,
artist_longitude DECIMAL,
artist_location TEXT);
""")

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (
                             songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
                             start_time timestamp NOT NULL,
                             user_id integer NOT NULL,
                             level char(4),
                             song_id char(18) NOT NULL,
                             artist_id char(18) NOT NULL,
                             session_id integer,
                             location varchar,
                             user_agent varchar
                             );
                        """)

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (
                         user_id integer PRIMARY KEY,
                         first_name varchar,
                         last_name varchar,
                         gender varchar,
                         level char(4)
                         );
                     """)

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (
                         song_id char(18) PRIMARY KEY,
                         title varchar,
                         artist_id char(18),
                         year smallint,
                         duration numeric(9, 5)
                         );
                     """)

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (
                           artist_id varchar(18) PRIMARY KEY,
                           name varchar,
                           location varchar,
                           latitude numeric(7, 5),
                           longitude numeric(8, 5)
                           );
                       """)

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (
                         start_time timestamp PRIMARY KEY,
                         hour smallint,
                         day smallint,
                         week smallint,
                         month smallint,
                         year smallint,
                         weekday boolean
                         );
                     """)

# STAGING TABLES

staging_events_copy = (f"""
COPY event_staging
FROM '{LOG_DATA}'
IAM_ROLE '{IAM_ARN}'
TIMEFORMAT AS 'epochmillisecs'
TRUNCATECOLUMNS
JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
COPY song_staging
FROM '{SONG_DATA}'
IAM_ROLE '{IAM_ARN}'
TRUNCATECOLUMNS
JSON 'auto';
""")

# TEST STAGING TABLES

test_staging_events_copy = (f"""
COPY event_staging
FROM '{LOG_DATA + "/2018/11/2018-11-01-events.json"}'
IAM_ROLE '{IAM_ARN}'
TIMEFORMAT AS 'epochmillisecs'
TRUNCATECOLUMNS
JSON '{LOG_JSONPATH}';
""")

test_staging_songs_copy = (f"""
COPY song_staging
FROM '{SONG_DATA + "/A"}'
IAM_ROLE '{IAM_ARN}'
TRUNCATECOLUMNS
JSON 'auto';
""")

# FINAL TABLES
#
# Note: Redshift does not respect primary keys
# so duplicates are handled by the insert statements.
#
# In particular, the users table contains the information
# from the most recent event log with that user_id
# and the artists table contains the information from
# an arbitrary song JSON containing that artist.
#
# All other tables have duplicates removed by using
# SELECT DISTINCT

songplay_table_insert = """
INSERT INTO songplays
(start_time, user_id, level, song_id,
  artist_id, session_id, location, user_agent)
SELECT e.ts, CAST(e.userId AS INT), e.level, s.song_id, s.artist_id,
  CAST(e.sessionId AS INTEGER), e.location, e.userAgent
FROM event_staging as e
JOIN song_staging as s
ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page = 'NextSong';
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT a, b, c, d, e FROM
  (SELECT CAST(userId AS INT) AS a, firstName AS b,
   lastName AS c, gender AS d, level AS e,
   rank() OVER (PARTITION BY userId ORDER BY ts DESC) AS rnk
   FROM event_staging
   WHERE auth != 'Logged Out') as subquery
WHERE rnk = 1;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, date_part(h, ts), date_part(d, ts), date_part(w, ts),
date_part(mon, ts), date_part(y, ts), (CASE WHEN date_part(dow, ts) BETWEEN 1 AND 5 THEN true ELSE false END)
FROM event_staging;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT sstg.song_id, sstg.title, sstg.artist_id, sstg.year, sstg.duration
FROM song_staging as sstg;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT a, b, c, d, e FROM
(SELECT artist_id AS a, artist_name AS b, artist_location AS c,
    artist_latitude AS d, artist_longitude AS e,
    row_number() OVER (PARTITION BY artist_id) AS row_num
    FROM song_staging) as subquery
WHERE row_num = 1;
"""


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
test_copy_table_queries = [test_staging_songs_copy, test_staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
