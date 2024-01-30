import configparser
import logging
from pathlib import Path

 # Set the logging level
logging.basicConfig(level=logging.INFO)

# Load pararameters from dwh.cfg
path = Path(__file__)
ROOT_DIR = path.parent.absolute() # Use root path if calling script from a separate directory
config_path = Path(ROOT_DIR, 'dwh.cfg')
config = configparser.ConfigParser()
config.read_file(open(config_path))

REGION          = config.get("AWS","REGION_NAME")

LOG_DATA        = config.get("S3","LOG_DATA")
LOG_JSONPATH    = config.get("S3","LOG_JSONPATH")
SONG_DATA       = config.get("S3","SONG_DATA")

IAM_ROLE_ARN    = config.get("IAM","IAM_ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS f_songlplay;"
user_table_drop = "DROP TABLE IF EXISTS d_user;"
song_table_drop = "DROP TABLE IF EXISTS d_song;"
artist_table_drop = "DROP TABLE IF EXISTS d_artist;"
time_table_drop = "DROP TABLE IF EXISTS d_time;"

# CREATE TABLES

# Staging tables do not contain 'NOT NULL' constraints so as to
# avoid dirty data issues - we want all data from S3 to be ingested
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR(350),
        auth            VARCHAR (255),
        firstName       VARCHAR(255),
        gender          VARCHAR(1),
        itemInSession   SMALLINT,
        lastName        VARCHAR(255),
        length          FLOAT,
        level           VARCHAR(20),
        location        VARCHAR(255),
        method          VARCHAR(6),
        page            VARCHAR(255),
        registration    FLOAT,
        sessionId       SMALLINT,
        song            VARCHAR(255),
        status          SMALLINT,
        ts              BIGINT,
        userAgent       VARCHAR(255),
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id             VARCHAR(20) PRIMARY KEY,
        num_songs           INTEGER,
        artist_id           VARCHAR(20),
        artist_name         VARCHAR(350),
        artist_latitude     DECIMAL(9,6),
        artist_longitude    DECIMAL(9,6),
        artist_location     VARCHAR(350),
        title               VARCHAR(350),
        duration            FLOAT,
        year                SMALLINT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS d_user (
        user_id     INTEGER PRIMARY KEY SORTKEY DISTKEY,
        first_name  VARCHAR(255) NOT NULL,
        last_name   VARCHAR(255) NOT NULL,
        gender      VARCHAR(1),
        level       VARCHAR(20)
    );                  
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS d_song (
        song_id     VARCHAR(20) PRIMARY KEY SORTKEY DISTKEY,
        title       VARCHAR(350) NOT NULL,
        artist_id   VARCHAR(20) NOT NULL,
        year        SMALLINT NOT NULL,
        duration    FLOAT NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS d_artist (
        artist_id   VARCHAR(20) PRIMARY KEY SORTKEY DISTKEY,
        name        VARCHAR(350) NOT NULL,
        location    VARCHAR(350),
        latitude    DECIMAL(9,6),
        longitude   DECIMAL (9,6)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS d_time (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        SMALLINT NOT NULL,  
        day         SMALLINT NOT NULL,
        week        SMALLINT NOT NULL,
        month       SMALLINT NOT NULL,
        year        SMALLINT NOT NULL,
        weekday     SMALLINT NOT NULL
    ) DISTSTYLE ALL;
""")

# Include this table last since it has reference dependencies
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS f_songplay (
        songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
        start_time  TIMESTAMP NOT NULL SORTKEY REFERENCES d_time(start_time),
        user_id     INTEGER NOT NULL REFERENCES d_user(user_id),
        level       VARCHAR(20),
        song_id     INTEGER NOT NULL REFERENCES d_song(song_id),
        artist_id   VARCHAR(20) NOT NULL REFERENCES d_artist(artist_id),
        session_id  SMALLINT NOT NULL,
        location    VARCHAR(255),
        user_agent  VARCHAR(255)
    ) DISTSTYLE auto distkey (start_time);
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events
    FROM '{LOG_DATA}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    REGION '{REGION}'
    JSON '{LOG_JSONPATH}';
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM '{SONG_DATA}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    REGION '{REGION}'
    JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO f_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        e.ts,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s ON (
            LOWER(TRIM(e.song = s.title)) 
            AND LOWER(TRIM(e.artist = s.artist_name))
        )
    AND e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO d_user (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT e.userId,
        e.firstName,
        e.lastName,
        e.gender,
        e.level
    FROM staging_events e
    WHERE e.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO d_song (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO d_artist (artist_id, name, location, latitude, longitude)
    SELECT
        DISTINCT s.artist_id,
        s.artist_name,
        s.artist_location,
        s.artist_latitude,
        s.artist_longitude
    FROM staging_songs s
    WHERE s.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO d_time (start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT s.start_time,
        EXTRACT(hour FROM s.start_time),
        EXTRACT(day FROM s.start_time),
        EXTRACT(week FROM s.start_time),
        EXTRACT(month FROM s.start_time),
        EXTRACT(year FROM s.start_time),
        EXTRACT(dayofweek FROM s.start_time)
    FROM f_songplay s;
""")

# QUERY LISTS


drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

