from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import isnan, count, when, col, desc, udf, col, sort_array, asc, avg
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StructType


def load_song_data(spark: SparkSession) -> DataFrame:
    path = "data/song_data"
    df = spark.read.json(path)
    return df


def load_log_data(spark: SparkSession) -> DataFrame:
    path = "data/log_data"
    df = spark.read.json(path)
    return df


def write_log_data(df: DataFrame, bucket: str, prefix: str):
    time_path = datetime.now()
    file_type = 'json'
    date_prefix = f"{time_path.year}{time_path.month}{time_path.day}"
    path = f"{bucket}/{prefix}"
    df.write.format(file_type).mode('overwrite').save(path)


def read_log_data(spark: SparkSession, bucket: str, prefix: str, schema: StructType) -> DataFrame:
    path = f"/Users/christopherlomeli/Source/courses/udacity/data-engineer/udacity-spark-model/{bucket}/{prefix}"
    path = f"{bucket}/{prefix}"
    df = spark.read.schema(schema=schema).json(path)


    return df

