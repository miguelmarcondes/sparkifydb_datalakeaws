import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id, dayofweek
from pyspark.sql.types import TimestampType, DateType, IntegerType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.createOrReplaceTempView("songs")
    songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])
    
    # extract columns to create artists table
    columns = ["artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    columns = [column + ' as ' + column.replace('artist_', '') for column in columns]
    artist_table = df.selectExpr("artist_id", *columns) 
    
    # write artists table to parquet files
    
    artist_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    full_df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = full_df.selectExpr('userId as user_id',
                                     'firstName as first_name',
                                     'lastName as last_name',
                                     'gender',
                                     'level').dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.createOrReplaceTempView("users")
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = func.udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    full_df = full_df.withColumn('start_time', get_timestamp('ts').cast(TimestampType()))
      
    # extract columns to create time table
    time_table = full_df.select('start_time')
    time_table = time_table.withColumn('hour', func.hour('start_time'))
    time_table = time_table.withColumn('day', func.dayofmonth('start_time'))
    time_table = time_table.withColumn('week', func.weekofyear('start_time'))
    time_table = time_table.withColumn('month', func.month('start_time'))
    time_table = time_table.withColumn('year', func.year('start_time'))
    time_table = time_table.withColumn('weekday', func.dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.createOrReplaceTempView("time")
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, "time"))

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("song_data")
    full_df.createOrReplaceTempView("log_data")
    
    # probably better to use row_number to increase the product's value
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                ld.start_time as start_time,
                                year(ld.start_time) as year,
                                month(ld.start_time) as month,
                                ld.userId as user_id,
                                ld.level as level,
                                sd.song_id as song_id,
                                sd.artist_id as artist_id,
                                ld.sessionId as session_id,
                                ld.location as location,
                                ld.userAgent as user_agent
                                FROM log_data ld
                                JOIN song_data sd
                                ON (ld.song = sd.title
                                AND ld.length = sd.duration
                                AND ld.artist = sd.artist_name)
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://udacity-dend-song-log/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print("Done!")
if __name__ == "__main__":
    main()
