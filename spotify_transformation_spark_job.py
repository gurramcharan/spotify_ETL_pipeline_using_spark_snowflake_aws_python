
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import explode,col,to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
s3_path = "s3://spotify-etl-spark-project/raw_data/to_processed/"
source_df = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json"
)
spotify_df = source_df.toDF()
df_artist_exploded = spotify_df.withColumn("items", explode("items")).select(explode(col("items.track.artists")).alias("artists"))
df = df_artist_exploded.select(
        col("artists.id").alias("artist_id"),
        col("artists.name").alias("artist_name"),
        col("artists.external_urls.spotify").alias("url"),
).drop_duplicates(['artist_id']).show(5,False)
def process_album(df):
    df = df.withColumn("items",explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url"),
    ).drop_duplicates(['album_id'])
    return df

def process_artists(df):
    df_artist_exploded = df.withColumn("items", explode("items")).select(explode(col("items.track.artists")).alias("artists"))
    df = df_artist_exploded.select(
        col("artists.id").alias("artist_id"),
        col("artists.name").alias("artist_name"),
        col("artists.external_urls.spotify").alias("url"),
    ).drop_duplicates(['artist_id'])
    return df

def process_songs(df):
    df_songs_exploded = df.select(explode(col("items")).alias("items"))
    df = df_songs_exploded \
    .withColumn("artist_id", explode(col("items.track.artists.id"))) \
    .select(
        col("items.track.id").alias("song_id"),
        col("items.track.name").alias("song_name"),
        col("items.track.duration_ms").alias("duration_ms"),
        col("items.track.external_urls.spotify").alias("url"),
        col("items.track.popularity").alias("popularity"),
        col("items.added_at").alias("songs_added"),
        col("items.track.album.id").alias("album_id"),
        col("artist_id")
    ).drop_duplicates(['song_id', 'artist_id'])
    df = df.withColumn("songs_added", to_date("songs_added"))
    return df
album_df = process_album(spotify_df)

artist_df = process_artists(spotify_df)

songs_df = process_songs(spotify_df)

def write_to_s3(df,path_suffix,format_type = "csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://spotify-etl-spark-project/transformed_data/{path_suffix}/"},
        format = format_type
    )
write_to_s3(album_df, "album_data/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(artist_df, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")
write_to_s3(songs_df, "songs_data/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")),"csv")


def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket = bucket, Prefix = prefix)
    keys = [content['Key'] for content in response.get('Contents',[]) if content['Key'].endswith('.json')]
    return keys
    
bucket_name = "spotify-etl-spark-project"
prefix = "raw_data/to_processed/"
spotify_keys = list_s3_objects(bucket_name,prefix)

def move_and_delete_files(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key':key
        }
        
        destination_key = 'raw_data/processed/' + key.split("/")[-1]
        
        s3_resource.meta.client.copy(copy_source,Bucket,destination_key)
        
        s3_resource.Object(Bucket,key).delete()
        
move_and_delete_files(spotify_keys,bucket_name)


job.commit()