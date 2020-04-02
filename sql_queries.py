import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")
JSON_PATH = config.get("S3", "LOG_JSONPATH")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS stg_events (artist varchar(200) , \
                                                          auth varchar(20), \
                                                          firstName varchar(50), \
                                                          gender CHAR(1), \
                                                          itemInSession Integer ,\
                                                          lastName varchar(50),\
                                                          length double precision,\
                                                          level varchar(10),\
                                                          location varchar(200),\
                                                          method varchar(10),\
                                                          page varchar(30),\
                                                          registration varchar(30),\
                                                          sessionId Integer,\
                                                          song varchar(200),\
                                                          status Integer,\
                                                          ts timestamp,\
                                                          userAgent varchar(200),\
                                                          userId Integer);""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stg_songs (song_id varchar(60),  \
                                        num_songs Integer,\
                                        artist_id varchar(60), \
                                        artist_latitude double precision, \
                                        artist_longitude double precision, \
                                        artist_location varchar(2560), \
                                        artist_name varchar(2560), \
                                        title varchar(2560), \
                                        duration double precision,\
                                        year Integer );""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id Integer IDENTITY PRIMARY KEY, \
                                                                  start_time timestamp NOT NULL sortkey, \
                                                                  user_id Integer  NOT NULL distkey, \
                                                                  level varchar(10) , \
                                                                  song_id varchar(60) NOT NULL, \
                                                                  artist_id varchar(60) NOT NULL, \
                                                                  session_id Integer , \
                                                                  location varchar(2560), \
                                                                  user_agent varchar(200));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id Integer  PRIMARY KEY NOT NULL sortkey, \
                                                          first_name varchar(50), \
                                                          last_name varchar(50), \
                                                          gender CHAR(1), \
                                                          level varchar(10));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar(60) PRIMARY KEY NOT NULL sortkey, \
                                                    title varchar(2560), \
                                                    artist_id varchar(60) NOT NULL, \
                                                    year Integer , \
                                                    duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar(60) PRIMARY KEY NOT NULL sortkey, \
                                                     artist_name varchar(2560), \
                                                     artist_location varchar(2560), \
                                                     artist_latitude double precision, \
                                                     artist_longitude double precision);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY NOT NULL sortkey, \
                                                         hour  Integer , \
                                                         day Integer , \
                                                         week Integer , \
                                                         month Integer , \
                                                         year Integer , \
                                                         weekday Integer );""")

# STAGING TABLES

staging_events_copy = ("""copy stg_events from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json '{}'
    timeformat 'epochmillisecs' 
    TRUNCATECOLUMNS;""").format(LOG_DATA,DWH_ROLE_ARN,JSON_PATH)



staging_songs_copy = ("""copy stg_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT as JSON 'auto'
    maxerror as 100
    TRUNCATECOLUMNS;""").format(SONG_DATA, DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) \
                            select distinct ts, userId, level, song_id, artist_id, sessionId, location, userAgent\
                            from ( select stg_events.ts, stg_events.userId,stg_events.level, stg_songs.song_id,\
                                   stg_songs.artist_id,stg_events.sessionId, stg_events.location, stg_events.userAgent, \
                                   ROW_NUMBER () OVER ( PARTITION BY stg_events.userId,level ORDER BY stg_events.ts desc ) rnk\
                                   from stg_events LEFT OUTER JOIN stg_songs \
                                   ON  stg_events.song = stg_songs.title \
                                   AND stg_events.artist = stg_songs.artist_name \
                                   AND stg_events.length = stg_songs.duration \
                                   where stg_events.userId is not NULL and stg_events.ts is not null \
                                   AND stg_songs.artist_id is not NULL AND stg_songs.song_id is not NULL \
                                ) where rnk =1;""")

user_table_insert = ("""INSERT INTO users \
                 SELECT userId, firstName,lastName,gender,level from (\
                 SELECT userId, firstName,lastName,gender,level,\
                 ROW_NUMBER () OVER ( PARTITION BY userId,level ORDER BY ts desc ) rnk\
                 FROM stg_events where userId is not null\
                 ) where rnk =1;""")

song_table_insert = ("""INSERT INTO songs \
                 select song_id,title,artist_id,year,duration from stg_songs \
                 where song_id is not null and artist_id is not null;""")

artist_table_insert = ("""INSERT INTO artists \
                 select artist_id,artist_name,artist_location,artist_latitude,artist_longitude \
                 from stg_songs where artist_id is not null;""")

time_table_insert = ("""INSERT INTO time \
                 select ts, extract(hour from ts) as hour,  extract(day from ts) as day, extract(week from ts) as week,\
                 extract(month from ts) as month,extract(year from ts) as year,date_part(dow, ts) as weekday \
                 from stg_events where ts is not null;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]