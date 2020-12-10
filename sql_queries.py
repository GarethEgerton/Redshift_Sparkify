import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""                 
                                CREATE TABLE IF NOT EXISTS staging_events (
                                    artist varchar NOT NULL,
                                    auth varchar NOT NULL,
                                    firstName varchar, 
                                    gender varchar(1), 
                                    itemInSession int NOT NULL,
                                    lastName varchar,
                                    length float NOT NULL,
                                    level varchar NOT NULL,
                                    location varchar,
                                    method varchar(8),
                                    page varchar NOT NULL,
                                    registration int8,
                                    sessionId int NOT NULL,
                                    song varchar NOT NULL,
                                    status int,
                                    ts bigint NOT NULL,
                                    useragent varchar,
                                    userId int NOT NULL
                                );
""")

staging_songs_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                    artist_id varchar NOT NULL,
                                    artist_latitude float,
                                    artist_location varchar,
                                    artist_longitude float,
                                    artist_name varchar NOT NULL,
                                    duration float NOT NULL,
                                    num_songs int,
                                    song_id varchar NOT NULL,
                                    title varchar NOT NULL,
                                    year int NOT NULL
                                    );
""")

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplay (
                                songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                                start_time timestamp NOT NULL, 
                                user_id int NOT NULL, 
                                level varchar, 
                                song_id varchar NOT NULL, 
                                artist_id varchar NOT NULL, 
                                session_id int NOT NULL, 
                                location varchar, 
                                user_agent varchar
                                );
                        """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id INT PRIMARY KEY, 
                            first_name varchar, 
                            last_name varchar, 
                            gender varchar(1), 
                            level varchar
                        );
""")

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS song (
                            song_id varchar PRIMARY KEY, 
                            title varchar NOT NULL, 
                            artist_id varchar NOT NULL, 
                            year int NOT NULL, 
                            duration float NOT NULL
                        );
""")

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artist (
                                artist_id varchar PRIMARY KEY, 
                                name varchar NOT NULL, 
                                location varchar, 
                                latitude float, 
                                longitude float
                            );
""")

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY, 
                            hour int NOT NULL, 
                            day int NOT NULL, 
                            week int NOT NULL, 
                            month int NOT NULL, 
                            year int NOT NULL, 
                            weekday in NOT NULL
                        );
""")

# STAGING TABLES
staging_events_copy = ("""
                            COPY staging_events FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            FORMAT AS JSON {};
                        """).format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
                            COPY staging_songs FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            JSON 'auto';
                        """).format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)


# FINAL TABLES
songplay_table_insert = ("""
                            INSERT INTO songplay (
                                start_time PRIMARY KEY, 
                                user_id NOT NULL, 
                                level, 
                                song_id NOT NULL, 
                                artist_id NOT NULL, 
                                session_id NOT NULL, 
                                location, 
                                user_agent)
                            SELECT 
                                TIMESTAMP 'epoch' +  e.ts/1000 * INTERVAL '1 second' as start_time, 
                                e.userid,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.sessionid,
                                e.location,
                                e.useragent
                            FROM 
                                staging_events e
                            JOIN staging_songs s
                                ON (e.song = s.title
                                     AND
                                    e.artist = s.artist_name
                                     and
                                    s.duration = e.length)
                            WHERE page = 'NextSong'
""")

user_table_insert = ("""
                        INSERT INTO users (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level
                        )
                        SELECT DISTINCT
                            userid,
                            firstname,
                            lastname,
                            gender,
                            level 
                        FROM staging_events
""")

song_table_insert = ("""
                        INSERT INTO song (
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        )
                        SELECT DISTINCT
                            e.song_id,
                            e.title,
                            e.artist_id,
                            e.year,
                            e.duration
                        FROM staging_songs e
""")

artist_table_insert = ("""
                        INSERT INTO artist (
                            artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude)
                        SELECT DISTINCT
                            artist_id,
                            artist_name, 
                            artist_location,
                            artist_latitude,
                            artist_longitude
                        FROM
                            staging_songs                        
""")

time_table_insert = ("""INSERT INTO time (
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year, 
                            weekday)
                        SELECT DISTINCT start_time,
                            EXTRACT (HOUR FROM start_time), 
                            EXTRACT (DAY FROM start_time),
                            EXTRACT (WEEK FROM start_time), 
                            EXTRACT (MONTH FROM start_time), 
                            EXTRACT (YEAR FROM start_time), 
                            EXTRACT (WEEKDAY FROM start_time) 
                        FROM
                            (SELECT 
                                TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
                            FROM 
                                staging_events);
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
