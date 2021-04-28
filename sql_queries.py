import configparser
import aws_utils


# CONFIG
config = aws_utils.get_s3_config()
LOG_DATA = config['LOG_DATA']
LOG_JSONPATH = config['LOG_JSONPATH']
SONG_DATA = config['SONG_DATA']

_, IAM_ARN = aws_utils.get_role_name_arn()

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
CREATE UNLOGGED TABLE event_staging (
id IDENTITY(0, 1),
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
CREATE UNLOGGED TABLE song_staging (
id IDENTITY(0, 1),
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
                             songplay_id serial PRIMARY KEY,
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
JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
COPY song_staging
FROM '{SONG_DATA}'
IAM_ROLE '{IAM_ARN}'
JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays
(start_time, user_id, level, song_id,
  artist_id, session_id, location, user_agent)
SELECT e.ts, CAST(e.userId AS INT), e.level, s.song_id, s.artist_id,
  CAST(e.sessionId AS INTEGER), e.location, e.userAgent
FROM event_staging as e
JOIN song_staging as s
ON e.song = s.title AND e.artist = s.artist_name
ON CONFLICT DO NOTHING;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT a, b, c, d, e FROM
  (SELECT CAST(userId AS INT) AS a, firstName AS b,
   lastName AS c, gender AS d, level AS e,
   rank() OVER (PARTITION BY userId ORDER BY ts DESC) AS rnk
   FROM event_staging) as subquery
WHERE rnk = 1
ON CONFLICT (user_id)
DO UPDATE SET (first_name, last_name, gender, level) =
(EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level);
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT e.ts, e.hour, e.day, e.week,
  e.month, e.year, e.weekday
FROM event_staging as e
ON CONFLICT DO NOTHING;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT sstg.song_id, sstg.title, sstg.artist_id, sstg.year, sstg.duration
FROM song_staging as sstg
ON CONFLICT DO NOTHING;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT sstg.artist_id, sstg.artist_name, sstg.artist_location,
  sstg.artist_latitude, sstg.artist_longitude
FROM song_staging as sstg
ON CONFLICT DO NOTHING;
"""

# FOREIGN KEYS AND INDICES
set_fk1 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__time
FOREIGN KEY (start_time)
REFERENCES time
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk2 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__users
FOREIGN KEY (user_id)
REFERENCES users
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk3 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__songs
FOREIGN KEY (song_id)
REFERENCES songs
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk4 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__artists
FOREIGN KEY (artist_id)
REFERENCES artists
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk5 = ("""
ALTER TABLE songs
ADD CONSTRAINT fk__songs__artists
FOREIGN KEY (artist_id)
REFERENCES artists
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_idx1 = "CREATE INDEX IF NOT EXISTS idx_start_time ON songplays (start_time);"
set_idx2 = "CREATE INDEX IF NOT EXISTS idx_user_id ON songplays (user_id);"
set_idx3 = "CREATE INDEX IF NOT EXISTS idx_song_id ON songplays (song_id);"
set_idx4 = "CREATE INDEX IF NOT EXISTS idx_artist_id ON songplays (artist_id);"
set_idx5 = "CREATE INDEX IF NOT EXISTS idx_artist_id ON songs (artist_id);"


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
fk_queries = [set_fk1, set_fk2, set_fk3, set_fk4, set_fk5]
idx_queries = [set_idx1, set_idx2, set_idx3, set_idx4, set_idx5]
