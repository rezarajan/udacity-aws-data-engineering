import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
        artist          VARCHAR(255),
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
        ts              TIMESTAMP,
        userAgent       VARCHAR(255),
        userId          INTEGER
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id             VARCHAR(20) PRIMARY KEY,
        num_songs           INTEGER,
        artist_id           VARCHAR(20),
        artist_name         VARCHAR(255),
        artist_latitude     DECIMAL(9,6),
        artist_longitude    DECIMAL(9,6),
        artist_location     VARCHAR(255),
        title               VARCHAR(255),
        duration            FLOAT,
        year                SMALLINT
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
