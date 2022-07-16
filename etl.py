import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType

RUN_ENVIRONMENT = "EMR"
APP_NAME = "cjl-udacity-project"
SPARK_VERSION = 3

# create_spark_session
spark = SparkSession\
    .builder\
    .appName(APP_NAME) \
    .getOrCreate()


if RUN_ENVIRONMENT == "EMR":
    # STAGE_DIRECTORY = "s3:/udacity-dend"
    # STAGE_DIRECTORY = "s3://cjl-udacity"
    STAGE_DIRECTORY = "s3://cjl-spark-stage"
    LAKE_DIRECTORY = "s3://cjl-spark-datalake"
    BAD_RECORDS = "s3://cjl-udacity/bad_records"
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    logging = log4jLogger.LogManager.getLogger("ETL")
else:
    STAGE_DIRECTORY = "data"
    LAKE_DIRECTORY = "lake"
    BAD_RECORDS = "bad_records"
    import logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)


class MyFileSystem:

    output_file_type: str = "parquet"
    schemas: dict = {
        "song_data": StructType.fromJson(json.loads("""{"fields":[{"metadata":{},"name":"artist_id","nullable":true,"type":"string"},{"metadata":{},"name":"artist_latitude","nullable":true,"type":"double"},{"metadata":{},"name":"artist_location","nullable":true,"type":"string"},{"metadata":{},"name":"artist_longitude","nullable":true,"type":"double"},{"metadata":{},"name":"artist_name","nullable":true,"type":"string"},{"metadata":{},"name":"duration","nullable":true,"type":"double"},{"metadata":{},"name":"num_songs","nullable":true,"type":"long"},{"metadata":{},"name":"song_id","nullable":true,"type":"string"},{"metadata":{},"name":"title","nullable":true,"type":"string"},{"metadata":{},"name":"year","nullable":true,"type":"long"}],"type":"struct"}""")),
        "log_data": StructType.fromJson(json.loads("""{"fields":[{"metadata":{},"name":"artist","nullable":true,"type":"string"},{"metadata":{},"name":"auth","nullable":true,"type":"string"},{"metadata":{},"name":"firstName","nullable":true,"type":"string"},{"metadata":{},"name":"gender","nullable":true,"type":"string"},{"metadata":{},"name":"itemInSession","nullable":true,"type":"long"},{"metadata":{},"name":"lastName","nullable":true,"type":"string"},{"metadata":{},"name":"length","nullable":true,"type":"double"},{"metadata":{},"name":"level","nullable":true,"type":"string"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"method","nullable":true,"type":"string"},{"metadata":{},"name":"origSong","nullable":true,"type":"string"},{"metadata":{},"name":"page","nullable":true,"type":"string"},{"metadata":{},"name":"registration","nullable":true,"type":"double"},{"metadata":{},"name":"sessionId","nullable":true,"type":"long"},{"metadata":{},"name":"song","nullable":true,"type":"string"},{"metadata":{},"name":"status","nullable":true,"type":"long"},{"metadata":{},"name":"ts","nullable":true,"type":"long"},{"metadata":{},"name":"userAgent","nullable":true,"type":"string"},{"metadata":{},"name":"userId","nullable":true,"type":"string"}],"type":"struct"}"""))
    }

    def __init__(self, input_data: str = "data", output_data: str = "output", perform_validations : bool = True):
        self.input_folder = input_data
        self.output_folder = output_data
        self.perform_validations = perform_validations

    def read_fs_data(self, prefix: str ) -> DataFrame:
        path = f"""{self.input_folder}/{prefix}"""
        if SPARK_VERSION == 2:
          if prefix == "song_data":
              path = f"""{self.input_folder}/{prefix}/*/*/*/*.json"""
          else:
              path = f"""{self.input_folder}/{prefix}/*/*/*.json"""

        logging.info(f"READING from : {path}")
        df = spark.read \
            .schema(self.schemas[prefix]) \
            .option("recursiveFileLookup", "true")\
            .json(path)
        logging.info(f"""READ {df.count()} song records """)
        # logging.info(f"SCHEMA: {df.schema.json()}")
        return df

    def write_fs_data(self, df: DataFrame, prefix: str):
        path = f"{self.output_folder}/{prefix}"
        df.write.format(self.output_file_type).mode('overwrite').save(path)
        logging.info(f"""WRITING to path: {path},    counted {df.count()} records""")

        if self.perform_validations:
            self.verify_written_data(path=path, schema=df.schema)

    def verify_written_data(self, path: str, schema: StructType) -> DataFrame:
        df = spark.read.format(self.output_file_type).schema(schema=schema).load(path)
        logging.info(f"""VALIDATE : {path},        read back {df.count()} records""")
        return df


def process_song_data(fs: MyFileSystem) -> DataFrame:

    # get filepath to song data file - append to fs
    song_data = "song_data"

    # read song data file
    stage_song_df = fs.read_fs_data(prefix=song_data)
    stage_song_df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_df = spark.sql("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM song_data_table
        WHERE song_id is not null
         """)

    # write songs table to parquet files
    fs.write_fs_data(songs_df, "dim_song")

    # extract columns to create artists table
    artist_df = spark.sql("""
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM song_data_table where artist_id is not null
    """)

    # write artists table to parquet files
    fs.write_fs_data(artist_df, "dim_artist")

    return stage_song_df


def process_log_data(fs: MyFileSystem):

    # get filepath to log data file
    log_data = "log_data"

    # read log data file
    stage_log_df = fs.read_fs_data(prefix="log_data") \
        .withColumnRenamed("userId", "userIdString")

    # filter by actions for song plays and do transforms
    log_df = stage_log_df.filter(F.col("page") == "NextSong")\
        .withColumn('userId', F.col("userIdString").cast(IntegerType())) \
        .withColumn('startTs', (F.col("ts") / 1000).cast("timestamp")) \
        .withColumn('time_id', F.date_format(F.col('startTs'), "yyyyMMddHHmmss"))

    log_df.createOrReplaceTempView("log_data_table")

    # extract columns for users table
    user_df = spark.sql("""
    select distinct
        userId, firstName, lastName, gender, level
        from log_data_table
        where userId is not null and length(userId) > 0
    """)

    # write users table to parquet files
    fs.write_fs_data(user_df, "dim_user")

    # extract columns to create time table
    # get_timestamp = udf()
    is_weekend = F.udf(lambda x: x in [6, 0])

    time_df = log_df.select(["time_id", "startTs"]) \
        .dropDuplicates() \
        .withColumn("year", F.year(F.col("startTs"))) \
        .withColumn("month", F.month(F.col("startTs"))) \
        .withColumn("day", F.dayofmonth(F.col("startTs"))) \
        .withColumn("hour", F.hour(F.col("startTs"))) \
        .withColumn("isWeekend", is_weekend(F.dayofweek(F.col("startTs"))))

    # write time table to parquet files partitioned by year and month
    fs.write_fs_data(time_df, "dim_time")

    # read in song data to use for songplays table

    # extract columns from joined song and log datasets to create songplays table
    songplay_df = spark.sql("""
        with X as (select
        startTs
        , time_id
        , userId
        , level
        , nvl(S.song_id, null) as song_id -- on song == song
        , nvl(S.artist_id, null)  as artist_id-- on artistname == artist name
        , sessionId
        , location
        , userAgent
    from log_data_table L
    join song_data_table S on S.title = L.song )
      select
        time_id
        , startTs
        , userId
        , level
        , song_id -- on song == song
        , artist_id-- on artistname == artist name
        , sessionId
        , location
        , userAgent
    from X
    """)

    # write songplays table to parquet files partitioned by year and month
    fs.write_fs_data(songplay_df, "fact_songplays")


def main():
    # spark session is global so I declare logging based on environment
    # put all filesystem logic into a class
    fs = MyFileSystem(
        input_data=STAGE_DIRECTORY,
        output_data=LAKE_DIRECTORY,
        perform_validations=True)

    process_song_data(fs=fs)
    process_log_data(fs=fs)


if __name__ == "__main__":
    main()