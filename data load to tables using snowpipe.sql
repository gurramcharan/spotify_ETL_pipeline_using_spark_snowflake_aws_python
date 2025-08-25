create database spotify_db;


create or replace storage integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::762305182181:role/spotify-spark-snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('S3://spotify-etl-spark-project')
COMMENT = 'CREATING CONNECTION TO S3';


DESC integration s3_int;


create or replace file format csv_fileformat
type = csv
field_delimiter = ','
skip_header = 1
null_if = ('NULL','null')
empty_field_as_null = TRUE;


create or replace stage spotify_stage
url = 's3://spotify-etl-spark-project/transformed_data/'
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = csv_fileformat;


list @spotify_stage/songs;


-- album_id,album_name,release_date,total_tracks,url
CREATE or REPLACE table tbl_album(
album_id string,
album_name string,
release_date date,
total_tracks integer,
url string
);


-- artist_id,artist_name,url
CREATE or REPLACE table tbl_artist(
artist_id string,
artist_name string,
url string
);


-- song_id,song_name,duration_ms,url,popularity,songs_added,album_id,artist_id
CREATE or REPLACE table tbl_songs(
song_id string,
song_name string,
duration_ms int,
url string,
popularity int,
songs_added date,
album_id string,
artist_id string
);


select * from tbl_songs;

select * from tbl_artist;

select * from tbl_album;


copy into tbl_songs
from @spotify_stage/songs_data/songs_transformed_2025-08-21/run-1755785701832-part-r-00000;


copy into tbl_artist
from @spotify_stage/artist_data/artist_transformed_2025-08-21/run-1755785700168-part-r-00000;


copy into tbl_album
from @spotify_stage/album_data/album_transformed_2025-08-24/run-1756056937517-part-r-00000;



CREATE OR REPLACE schema pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
from @spotify_db.public.spotify_stage/songs_data/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_artist_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artist
from @spotify_db.public.spotify_stage/artist_data/;


CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
from @spotify_db.public.spotify_stage/album_data/;




desc pipe pipe.tbl_songs_pipe;

desc pipe pipe.tbl_album_pipe;

desc pipe pipe.tbl_artist_pipe;

select count(*) from tbl_songs;

select count(*) from tbl_artist;

select count(*) from tbl_album;


select system$pipe_status('pipe.tbl_songs_pipe');

select system$pipe_status('pipe.tbl_artist_pipe');

select system$pipe_status('pipe.tbl_album_pipe');