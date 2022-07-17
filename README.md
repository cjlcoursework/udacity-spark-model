# spark_etl

### This project contains
- `etl.py` -- main program for creating and loading our parquet cluster
- `data/` -- local laptop test data 
- `log/`  -- outputs 
  - `cjl-spark-stage.log` -- contains a listing of the files in `s3://cjl-spark-stage/*`
  - `console.log` -- contains the console output of a spark-

---
### Issues
- [ ] <font color='red'>EMR version 5.28.0 has `python2` by default -- rather than installing python3, I just point pyspark to `python3`</font>
  ```
    [hadoop@ip-172-31-19-83 ~]$ export PYSPARK_PYTHON=/usr/bin/python3
    [hadoop@ip-172-31-19-83 ~]$ export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
  ```

- [ ] <font color='red'>EMR version 5.28.0 has Spark 2 version while version 6 (eg. 6.50) has Spark 3</font>
  - `Spark 2` - some Spark versions did not support recursiveFileLookup -- I needed to get to a specific data level with wildcards e.g. `*/*/*.json`
  - `Spark 3` - recurse with recursiveFileLookup and just supply the root path

- [ ] <font color='red'>I copied the files from `s3:/udacity-dend` to my s3 bucket. </font> 
  - See `log/cjl-spark-stage.files.log` for the list of files</font>
  - Running directly against the `s3:/udacity-dend/song_data` dataset caused the following problems for me:
  ```
  22/07/17 20:47:15 INFO ETL: READING from : s3:/udacity-dend/song_data
  22/07/17 20:47:16 INFO ClientConfigurationFactory: Set initial getObject socket timeout to 2000 ms.
  Traceback (most recent call last):
  File "/home/hadoop/etl.py", line 222, in <module>
  main()
  File "/home/hadoop/etl.py", line 217, in main
  process_song_data(fs=fs)
  File "/home/hadoop/etl.py", line 103, in process_song_data
  stage_song_df = fs.read_fs_data(prefix=song_data)
  File "/home/hadoop/etl.py", line 73, in read_fs_data
  .json(path)
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 372, in json
  File "/usr/lib/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1305, in __call__
  File "/usr/lib/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 111, in deco
  File "/usr/lib/spark/python/lib/py4j-0.10.9-src.zip/py4j/protocol.py", line 328, in get_return_value
  py4j.protocol.Py4JJavaError: An error occurred while calling o68.json.
  : java.lang.NullPointerException: bucket is marked non-null but is null
  ```
 
- [ ] <font color='red'>Adding `partitionBy` seemed to add <u>significantly</u> to the load - especially for songs_data </font>
  - I needed 4 core instances to complete the large dataset.  I'm sure there's a lot of additional tuning possible.
---
##### <font color='green'>ETL Requirement: </font>
- [ ] Using pyspark running on an EMR cluster<br>
- [ ] Read log_data and song_date from a public s3 directory
  - `s3:/udacity-dend/song_data` contains artist and song data for each song
  - `s3:/udacity-dend/log_data` contains events where users listed to songs<br>
- [ ] Transform t the following DataFrames:
  - `fact_songplays` --  events where users have 'listened' to a song
  - `dim_song` -- information about each song, such as title, duration, etc.
  - `dim_user` -- a user is the persona that listened to each song
  - `dim_artist`  --- an artist is the person who created each song
  - `dim_time` -- the time dimension just breaks out the timestamp of each event into data parts<br>
- [ ] Transform into the following DataFrames:

---
##### <font color='green'>Steps: </font>
- AWS copy data from the `s3:/udacity-dend/*` to `s3://cjl-spark-stage/*`
- Create an EMR cluster (I used the console)
- ssh into the master node and copy etl.py into `/home/hadoop`
- Update the following variables in etl.py
  - <font color='yellow'>RUN_ENVIRONMENT</font> = "LOCAL" to test in the laptop,  or "EMR" to run on EMR
  - <font color='yellow'>EMR_VERSION</font> : int = 5 if we are running on emr version 5.*** or 6.*** - I tested with both
  - <font color='yellow'>STAGE_DIRECTORY</font> = <the root staging folder e.g. `s3://cjl-spark-stage/*`>
  - <font color='yellow'>LAKE_DIRECTORY</font> = <the root output folder e.g. `s3://cjl-spark-datalake`>
  - <font color='yellow'>FEATURE_PARTITIONED_WRITES</font> = whether to partition the writes using partitionBy - or ignore the partitions argument

- INSERT into the LAKE_DIRECTORY star schema containing the following tables:
  - `fact_songplays` --  events where users have 'listened' to a song
  - `dim_song` -- information about each song, such as title, duration, etc.
  - `dim_user` -- a user is the persona that listened to each song
  - `dim_artist`  --- an artist is the person who created each song
  - `dim_time` -- the time dimension just breaks out the timestamp of each event into data parts

---
##### <font color='yellow'>song_data schema:</font>
```
{
  "type": "struct"
  "fields": [
    {      "name": "artist_id",      "type": "string"    },
    {      "name": "artist_latitude",      "type": "double"    },
    {      "name": "artist_location",      "type": "string"    },
    {      "name": "artist_longitude",      "type": "double"    },
    {      "name": "artist_name",      "type": "string"    },
    {      "name": "duration",      "type": "double"    },
    {      "name": "num_songs",      "type": "long"    },
    {      "name": "song_id",      "type": "string"    },
    {      "name": "title",      "type": "string"    },
    {      "name": "year",      "type": "long"    }
  ],
}
```
---
##### <font color='yellow'>log_data schema:</font>
```
{
  "fields": [
    {      "name": "artist",      "type": "string"    },
    {      "name": "auth",      "type": "string"    },
    {      "name": "firstName",      "type": "string"    },
    {      "name": "gender",      "type": "string"    },
    {      "name": "itemInSession",      "type": "long"    },
    {      "name": "lastName",      "type": "string"    },
    {      "name": "length",      "type": "double"    },
    {      "name": "level",      "type": "string"    },
    {      "name": "location",      "type": "string"    },
    {      "name": "method",      "type": "string"    },
    {      "name": "origSong",      "type": "string"    },
    {      "name": "page",      "type": "string"    },
    {     "name": "registration",      "type": "double"    },
    {       "name": "sessionId",      "type": "long"    },
    {      "name": "song",      "type": "string"    },
    {      "name": "status",      "type": "long"    },
    {      "name": "ts",      "type": "long"    },
    {      "name": "userAgent",      "type": "string"    },
    {      "name": "userId",      "type": "string"    }
  ],
  "type": "struct"
}
```


