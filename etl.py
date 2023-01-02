import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the set of songs json files from S3. Save the dataframes processed in parquet fomrat in S3.
    """
    song_data = input_data+'song_data/*/*/*/*.json'
    
    df = spark.read.json(song_data)

    fields = ['song_id','title','artist_id','year','duration']
    songs_table = df.select(fields).dropDuplicates()
    
    songs_table.write.partitionBy(['year','artist_id']).parquet(output_data + 'songs/')

    artists_table = df.select([df.artist_id, 
                                df.artist_name.alias('name'), 
                                df.artist_location.alias('location'), 
                                df.artist_latitude.alias('latitude'), 
                                df.artist_longitude.alias('longitude')]).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Process the set of logs files from S3. Save the dataframes processed in parquet fomrat in S3.
    """
 
    log_data = input_data+'log_data/*/*/*.json'

    df = spark.read.json(log_data) 
    
    df = df.filter(df.page == "NextSong")
  
    users_table = df.select([df.userId.alias('user_id'), 
                             df.firstName.alias('first_name'), 
                             df.lastName.alias('last_name'), 
                             df.gender, 
                             df.level]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    time_table = df.select(df.start_time.alias('start_time'), 
                            hour(df.start_time).alias('hour'),
                            dayofmonth(df.start_time).alias('day'),
                            weekofyear(df.start_time).alias('week'),
                            month(df.start_time).alias('month'),
                            year(df.start_time).alias('year'),
                            dayofweek(df.start_time).alias('weekday'),
                            date_format(df.start_time, 'yyyy-MM-dd').alias('datetime')).dropDuplicates()
    

    time_table.write.partitionBy(['year','month']).parquet(output_data + 'time/')

    songs_df = spark.read.parquet(output_data  + 'songs/*')
    artists_df = spark.read.parquet(output_data + 'artists/*')

    songs_logs = df.join(songs_df, (df.song == songs_df.title))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))

    songplays = artists_songs_logs.join(
                                        time_table,
                                        artists_songs_logs.ts == time_table.start_time, 'left'
                                        )

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.select( col('start_time').alias('start_time'),
                                        col('userId').alias('user_id'),
                                        col('level').alias('level'),
                                        col('song_id').alias('song_id'),
                                        col('artist_id').alias('artist_id'),
                                        col('sessionId').alias('session_id'),
                                        col('location').alias('location'),
                                        col('userAgent').alias('user_agent'),
                                        col('year').alias('year'),
                                        col('month').alias('month')
                                    ).repartition("year", "month")

    songplays_table.write.partitionBy(['year','month']).parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend-victor/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
