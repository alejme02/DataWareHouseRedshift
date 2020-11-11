import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
staging_events_table_create= ("""
 CREATE TABLE staging_events_table (
        artist varchar(400),
        auth varchar(100),
        firstName varchar(100),
        gender char,
        itemInSession integer,
        lastName varchar(100),
        length decimal,
        level varchar(100),
        location varchar(400),
        method varchar(100),
        page varchar(100),
        registration decimal,
        sessionId integer,
        song varchar(400),
        status integer,
        ts bigint,
        userAgent varchar(400),
        userId integer)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table(
       artist_id varchar(100),
       artist_latitude varchar(400),
       artist_location varchar(400),
       artist_longitude varchar(400),
       artist_name varchar(100),
       duration decimal,
       num_songs integer, 
       song_id varchar(300),
       title varchar(300), 
       year integer)
""")

songplay_table_create = ("""
CREATE TABLE songplay_table (
       songplaykey bigint identity(0, 1) distkey,
       song_id varchar(100) NOT NULL,
       artist_id varchar(100) NOT NULL,
       start_time bigint NOT NULL sortkey,
       user_id integer NOT NULL,
       level varchar(100),
       session_id integer,
       location varchar(400),
       user_agent varchar(400))
""")

user_table_create = ("""
CREATE TABLE user_table(
       user_id integer NOT NULL distkey,
       first_name varchar(100) NOT NULL, 
       last_name varchar(100),
       gender char NOT NULL, 
       level varchar(100) NOT NULL)
""")

song_table_create = ("""
CREATE TABLE song_table(
       song_id varchar(100) NOT NULL distkey,
       title varchar(100) NOT NULL, 
       artist_id varchar(100) NOT NULL, 
       year integer sortkey, 
       duration decimal)
""")

artist_table_create = ("""
CREATE TABLE artist_table(
       artist_id varchar(100) NOT NULL distkey,
       name varchar(100) NOT NULL, 
       location varchar(400),
       latitude varchar(400), 
       longitude varchar(400))
""")

time_table_create = ("""
CREATE TABLE time_table(
       start_time bigint NOT NULL distkey,
       date datetime NOT NULL sortkey,
       hour integer NOT NULL, 
       day integer NOT NULL, 
       week integer NOT NULL,
       month integer NOT NULL, 
       year integer NOT NULL, 
       weekday integer NOT NULL)
""")


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration) 
    SELECT DISTINCT song_id, title, artist_id, year, duration 
    FROM staging_songs_table;
""")

user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events_table WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songplay_table(song_id,artist_id,start_time,user_id,level,session_id,location,user_agent)
    SELECT DISTINCT s.song_id, a.artist_id, st.ts, st.userId, st.level, st.sessionId, st.location, st.userAgent 
    FROM staging_events_table st 
    JOIN song_table s ON s.title = st.song AND s.duration = st.length
    JOIN artist_table a ON a.name = st.artist
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs_table;
""")

time_table_insert = ("""
INSERT INTO time_table(start_time,date,hour,day,week,month,year,weekday)
SELECT DISTINCT ts, DATEADD(MILLISECOND, ts % 1000, DATEADD(SECOND, ts / 1000, '19700101')) as date,
       EXTRACT(HOUR FROM date) as hour, 
       EXTRACT(DAY FROM date) as day,
       EXTRACT(WEEK FROM date) as week,                                              
       EXTRACT(MONTH FROM date) as month,  
       EXTRACT(YEAR FROM date) as year, 
       EXTRACT(WEEKDAY FROM date) as weekday                                             
       FROM staging_events_table
""")

dwh_arn = config.get("DWH","DWH_ARN")
song_data = config.get("S3","song_data")

staging_songs_copy = ("""
    COPY staging_songs_table
    FROM 's3://udacity-dend/song_data/'
    iam_role '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 'auto';
""").format(dwh_arn)

staging_events_copy = ("""
    COPY staging_events_table
    FROM 's3://udacity-dend/log_data'
    iam_role '{}'
    REGION 'us-west-2'
    COMPUPDATE OFF
    JSON 's3://udacity-dend/log_json_path.json';
""").format(dwh_arn)


# QUERY LISTS
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
create_table = [staging_songs_table_create, staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]