import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        firstName text,
        gender char(1),
        itemInSession int,
        lastName text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration VARCHAR,
        sessionId bigint,
        song text,
        status int,
        ts bigint NOT NULL,
        userAgent text,
        userId text
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id text,
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration float,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id text NOT NULL,
        level varchar(20),
        song_id text,
        artist_id text,
        session_id integer,
        location text,
        user_agent text,
        UNIQUE ( start_time , user_id , level , song_id , artist_id , session_id , location , user_agent )
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id text PRIMARY KEY,
        first_name text,
        last_name text,
        gender char(1),
        level varchar(20)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY,
        title text NOT NULL,
        artist_id text,
        year integer,
        duration double precision NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text PRIMARY KEY,
        name text NOT NULL,
        location text,
        latitude double precision,
        longitude double precision
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer,
        UNIQUE ( start_time )
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    FORMAT AS JSON {} 
    region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    FORMAT AS JSON 'auto'
    region 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent )
        SELECT 
            TIMESTAMP 'epoch' + (se.ts / 1000) * interval '1 second' as start_time, 
            se.userId as user_id,
            se.level as level,
            ss.song_id as song_id,
            ss.artist_id as artist_id,
            se.sessionId as session_id,
            se.location as location,
            se.userAgent as user_agent
        FROM staging_events se
            JOIN staging_songs ss ON ( se.artist = ss.artist_name AND se.song = ss.title )
        WHERE se.page = 'NextSong'
        
            
""")

user_table_insert = ("""
    INSERT INTO users ( user_id , first_name, last_name, gender ,level )
        SELECT DISTINCT
            se.userId as user_id,
            se.firstName as first_name,
            se.lastName as last_name,
            se.gender,
            se.level
        FROM staging_events se
        WHERE se.page = 'NextSong' AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs ( song_id, title, artist_id, year, duration )
        SELECT DISTINCT
            ss.song_id,
            ss.title,
            ss.artist_id as artist_id,
            ss.year, 
            ss.duration
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists ( artist_id, name, location, latitude, longitude )
        SELECT DISTINCT
            ss.artist_id,
            ss.artist_name as name,
            ss.artist_location as location,
            ss.artist_latitude as latitude,
            ss.artist_longitude as longitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time ( start_time, hour, day, week, month, year, weekday )
        SELECT DISTINCT
            TIMESTAMP 'epoch' + (ts/1000) * interval '1 second' as start_time,
            extract(hour from start_time) as hour,
            extract(day from start_time) as day,
            extract(week from start_time) as week, 
            extract(month from start_time) as month,
            extract(year from start_time) as year, 
            extract(dayofweek from start_time) as weekday
        FROM staging_events
            
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
