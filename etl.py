import json
from datetime import datetime
from typing import List, Union

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType
"""
- AWS copy data from the `s3:/udacity-dend/*` to `s3://cjl-spark-stage/*`
- Create an EMR cluster (I used the console)
- ssh into the master node and copy etl.py into `/home/hadoop`
- Update the following variables in etl.py
  - RUN_ENVIRONMENT</font> = "LOCAL" to test in the laptop,  or "EMR" to run on EMR
  - EMR_VERSION</font> : int = 5 if we are running on emr version 5.*** or 6.*** - I tested with both
  - STAGE_DIRECTORY</font> = <the root staging folder e.g. `s3://cjl-spark-stage/*`>
  - LAKE_DIRECTORY</font> = <the root output folder e.g. `s3://cjl-spark-datalake`>
  - FEATURE_PARTITIONED_WRITES</font> = whether to partition the writes using partitionBy - or ignore the partitions argument

- INSERT into the LAKE_DIRECTORY star schema containing the following tables:
  - `fact_songplays` --  events where users have 'listened' to a song
  - `dim_song` -- information about each song, such as title, duration, etc.
  - `dim_user` -- a user is the persona that listened to each song
  - `dim_artist`  --- an artist is the person who created each song
  - `dim_time` -- the time dimension just breaks out the timestamp of each event into data parts


"""
APP_NAME = "cjl-udacity-project"


# --------------------  Global Spark Session ---------------------
spark = SparkSession \
    .builder \
    .appName(APP_NAME) \
    .getOrCreate()

# --------------------  Configurations --------------------- - TODO move all of these into a configuration file
FEATURE_PARTITIONED_WRITES: bool = True
FEATURE_VALIDATION: bool = True
RUN_ENVIRONMENT = "EMR"
EMR_VERSION: int = 6  # 5 for version emr-5.*** and 6 for version emr-6***

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


# --------------------  Code --------------------- -

class MyFileSystem:
    """
    Class that handles all read/write logic
    """
    output_file_type: str = "parquet"
    schemas: dict = {
        "song_data": StructType.fromJson(json.loads(
            """{"fields":[{"metadata":{},"name":"artist_id","nullable":true,"type":"string"},{"metadata":{},"name":"artist_latitude","nullable":true,"type":"double"},{"metadata":{},"name":"artist_location","nullable":true,"type":"string"},{"metadata":{},"name":"artist_longitude","nullable":true,"type":"double"},{"metadata":{},"name":"artist_name","nullable":true,"type":"string"},{"metadata":{},"name":"duration","nullable":true,"type":"double"},{"metadata":{},"name":"num_songs","nullable":true,"type":"long"},{"metadata":{},"name":"song_id","nullable":true,"type":"string"},{"metadata":{},"name":"title","nullable":true,"type":"string"},{"metadata":{},"name":"year","nullable":true,"type":"long"}],"type":"struct"}""")),
        "log_data": StructType.fromJson(json.loads(
            """{"fields":[{"metadata":{},"name":"artist","nullable":true,"type":"string"},{"metadata":{},"name":"auth","nullable":true,"type":"string"},{"metadata":{},"name":"firstName","nullable":true,"type":"string"},{"metadata":{},"name":"gender","nullable":true,"type":"string"},{"metadata":{},"name":"itemInSession","nullable":true,"type":"long"},{"metadata":{},"name":"lastName","nullable":true,"type":"string"},{"metadata":{},"name":"length","nullable":true,"type":"double"},{"metadata":{},"name":"level","nullable":true,"type":"string"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"method","nullable":true,"type":"string"},{"metadata":{},"name":"origSong","nullable":true,"type":"string"},{"metadata":{},"name":"page","nullable":true,"type":"string"},{"metadata":{},"name":"registration","nullable":true,"type":"double"},{"metadata":{},"name":"sessionId","nullable":true,"type":"long"},{"metadata":{},"name":"song","nullable":true,"type":"string"},{"metadata":{},"name":"status","nullable":true,"type":"long"},{"metadata":{},"name":"ts","nullable":true,"type":"long"},{"metadata":{},"name":"userAgent","nullable":true,"type":"string"},{"metadata":{},"name":"userId","nullable":true,"type":"string"}],"type":"struct"}"""))
    }

    def __init__(self, input_data: str = "data", output_data: str = "output", perform_validations: bool = True):
        self.input_folder = input_data
        self.output_folder = output_data
        self.perform_validations = perform_validations

    def read_fs_data(self, prefix: str) -> DataFrame:
        """
        Given a prefix - such as "song_data" -read all files under the prefix
        :param prefix: a subdirectory such as song_data, or log_data
        :return: Return the DataFrame
        """
        path = f"""{self.input_folder}/{prefix}"""
        emr: int = int(EMR_VERSION)
        # version emr 5 with Spark 2 does not sometimes seem to handle recursive
        # - if that happens we can define a specific level to get the data from
        if emr < 6:
            if prefix == "song_data":
                path = f"""{self.input_folder}/{prefix}/*/*/*/*.json"""
            else:
                path = f"""{self.input_folder}/{prefix}/*/*/*.json"""

        logging.info(f"READING from : {path}")

        # perform the read
        df = spark.read \
            .schema(self.schemas[prefix]) \
            .option("recursiveFileLookup", "true") \
            .json(path)
        logging.info(f"""READ {df.count()} song records """)
        return df

    def write_fs_data(self, df: DataFrame, prefix: str, partitions=None):
        """
        Write a DataFram to the prefix (table) provided
            - partition the writes if required

        :param df: The DataFram to write
        :param prefix: the filesystem location (table) to write to
        :param partitions: a list of columns to partitionBy()

        """
        if partitions is None:
            partitions = []
        path = f"{self.output_folder}/{prefix}"

        # prepare the data frame writer
        dfw = df.write \
            .format(self.output_file_type) \
            .partitionBy(partitions) \
            .mode('overwrite')
        # conditionally all partitions
        if FEATURE_PARTITIONED_WRITES and len(partitions) == 0:
            dfw.partitionBy(*partitions)

        # Perform write
        dfw.save(path)
        logging.info(f"""WRITING to path: {path},    counted {df.count()} records""")

        if FEATURE_VALIDATION and self.perform_validations:
            self.verify_written_data(path=path, schema=df.schema)

    def verify_written_data(self, path: str, schema: StructType) -> DataFrame:
        """
        Read the written data back - to chck that we got the same counts

        :param path: the path to read from
        :param schema: the schema of the data we are reading

        :return: The DataFrame we read
        """
        df = spark.read.format(self.output_file_type).schema(schema=schema).load(path)
        logging.info(f"""VALIDATE : {path},        read back {df.count()} records""")
        return df


def process_song_data(fs: MyFileSystem) -> DataFrame:
    """
    Given a MyFileSystem object, containing read and write utilities,
    - read the song_data folder
    - create a tempView table of the data, so we can use SQL
    - create a dim_song dimension and write it to s3
    - create a dim_artist dimension and write it to s3

    :param fs: A MyFileSystem object

    :return: the song_data DatFrame in case we need it
    """
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
    fs.write_fs_data(songs_df, "dim_song", partitions=["year"])

    # extract columns to create artists table
    artist_df = spark.sql("""
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM song_data_table where artist_id is not null
    """)

    # write artists table to parquet files
    fs.write_fs_data(artist_df, "dim_artist")

    return stage_song_df


def process_log_data(fs: MyFileSystem):
    """
    Given a MyFileSystem object, containing read and write utilities,
    - read the log_data folder
    - create a tempView table of the data, so we can use SQL
    - create a dim_user dimension and write it to s3
    - create a dim_time dimension and write it to s3
    - create a fact_songplays dimension (by joining the song and log temp views) and write it to s3

    :param fs: A MyFileSystem object

    :return: the song_data DatFrame in case we need it
    """

    # get filepath to log data file
    log_data = "log_data"

    # read log data file
    stage_log_df = fs.read_fs_data(prefix="log_data") \
        .withColumnRenamed("userId", "userIdString")

    # filter by actions for song plays and do transforms
    log_df = stage_log_df.filter(F.col("page") == "NextSong") \
        .withColumn('userId', F.col("userIdString").cast(IntegerType())) \
        .withColumn('startTs', (F.col("ts") / 1000).cast("timestamp")) \
        .withColumn('time_id', F.date_format(F.col('startTs'), "yyyyMMddHHmmss")) \
        .withColumn("year", F.year(F.col("startTs"))) \
        .withColumn("month", F.month(F.col("startTs"))) \
        .withColumn("day", F.dayofmonth(F.col("startTs")))

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

    time_df = log_df.select(["time_id", "startTs", "year", "month", "day"]) \
        .dropDuplicates() \
        .withColumn("hour", F.hour(F.col("startTs"))) \
        .withColumn("isWeekend", is_weekend(F.dayofweek(F.col("startTs"))))

    # write time table to parquet files partitioned by year and month
    fs.write_fs_data(df=time_df, prefix="dim_time", partitions=["year", "month", "day"])

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
        , L.year
        , L.month
        , L.day
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
        , year
        , month
        , day
    from X
    """)

    # write songplays table to parquet files partitioned by year and month
    fs.write_fs_data(songplay_df, "fact_songplays", partitions=["year", "month", "day"])


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
