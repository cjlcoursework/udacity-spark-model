from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from spark_local_files import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('abc') \
        .getOrCreate()

    song_data_df = load_song_data(spark=spark)
    song_data_df.printSchema()
    song_data_df.createOrReplaceTempView("song_data_table")

    function = F.udf(lambda x: int(x))

    log_data_df = load_log_data(spark=spark) \
        .withColumnRenamed("userId", "userIdString")
    log_data_df = log_data_df \
        .withColumn('userId', F.col("userIdString").cast(IntegerType())) \
        .withColumn('startTs', (F.col("ts") / 1000).cast("timestamp")) \
        .withColumn('startDateStr', F.date_format(F.col('startTs'), "yyyyMMddHHmmss"))
    log_data_df.printSchema()
    log_data_df.show(n=2, truncate=False)

    log_data_df.createOrReplaceTempView("log_data_table")

    songs_df = spark.sql("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM song_data_table
        WHERE song_id is not null
         """)

    songs_df.printSchema()

    artist_df = spark.sql("""
    SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM song_data_table where artist_id is not null
    """)

    artist_df.printSchema()

    spark.udf.register("to_number", lambda x: int(x))

    user_df = spark.sql("""
    select distinct
        userId, firstName, lastName, gender, level
        from log_data_table
        where userId is not null and length(userId) > 0
    """)

    user_df.show(truncate=False)
    user_df.printSchema()

    songplay_df = spark.sql("""
        with X as (select 
        startTs
        , startDateStr
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
        startDateStr as time_id
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

    songplay_df.printSchema()
    songplay_df.show(n=2, truncate=False)

    is_weekend = F.udf(lambda x: x in [6, 0])

    time_df = songplay_df.select(["time_id", "startTs"]) \
        .dropDuplicates() \
        .withColumn("year", F.year(F.col("startTs"))) \
        .withColumn("month", F.month(F.col("startTs"))) \
        .withColumn("day", F.dayofmonth(F.col("startTs"))) \
        .withColumn("hour", F.hour(F.col("startTs"))) \
        .withColumn("isWeekend", is_weekend(F.dayofweek(F.col("startTs"))))

    time_count = time_df.count()
    song_count = songs_df.count()
    artist_count = artist_df.count()
    user_count = user_df.count()
    songplay_count = songplay_df.count()

    print(f"""*** READ ***\n
       time_count= {time_count}
       song_count= {song_count}
       artist_count= {artist_count}
       user_count= {user_count}
       songplay_count= {songplay_count}
    """)

    write_log_data(time_df, "output", "dim_time")
    write_log_data(songs_df, "output", "dim_song")
    write_log_data(artist_df, "output", "dim_artist")
    write_log_data(user_df, "output", "dim_user")
    write_log_data(songplay_df, "output", "fact_songplays")

    read_time_df = read_log_data(spark=spark, bucket="output", prefix="dim_time", schema=time_df.schema)
    read_song_df = read_log_data(spark=spark, bucket="output", prefix="dim_song", schema=songs_df.schema)
    read_artist_df = read_log_data(spark=spark, bucket="output", prefix="dim_artist", schema=artist_df.schema)
    read_user_df = read_log_data(spark=spark, bucket="output", prefix="dim_user", schema=user_df.schema)
    read_songplay_df = read_log_data(spark=spark, bucket="output", prefix="fact_songplays", schema=songplay_df.schema)

    print(f"""*** WRITTEN ***\n
       time_count= {read_time_df.count()}
       song_count= {read_song_df.count()}
       artist_count= {read_artist_df.count()}
       user_count= {read_user_df.count()}
       songplay_count= {read_songplay_df.count()}
    """)

