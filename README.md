# spark_etl

### This project contains
- `etl.py` -- main program for creating and loading our parquet cluster
- `data/` -- local laptop test data 
- `log/`  -- outputs 
  - `cjl-spark-stage.log` -- contains a listing of the files in `s3://cjl-spark-stage/*`
  - `console.log` -- contains the console output of a spark-

---
### Issues
- [ ] EMR version 5.28.0 has `python2` by default -- So I set environment variables to `python3`
  ```
    [hadoop@ip-172-31-19-83 ~]$ export PYSPARK_PYTHON=/usr/bin/python3
    [hadoop@ip-172-31-19-83 ~]$ export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
  ```

- [ ] EMR version 5.28.0 has Spark 2 version while version 6 (eg. 6.50) has Spark 3
  - Spark 2 - does not support recursiveFileLookup -- needed to get to the data level with wildcards e.g. `*/*/*,json`
  - Spark 3 - recurse with recursiveFileLookup and just supply the root path

- [ ] I copied the files from `s3:/udacity-dend` to my s3 bucket.  
  - <font color='green'>[See log/cjl-spark-stage.files.log] for the list of files</font>
 
- [ ] Adding partitionBy seemed to add significantly to the load - I needed 4 core instances to complete the large dataset.  I'm sure there's a lot of additional tuning possible.
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
  - RUN_ENVIRONMENT = "LOCAL" to test in the laptop,  or "EMR" to run on EMR
  - EMR_VERSION : int = 5 or 6 - tested with both
  - STAGE_DIRECTORY = <the root staging folder e.g. `s3://cjl-spark-stage/*`>
  - LAKE_DIRECTORY = <the root output folder e.g. `s3://cjl-spark-datalake`>
  - Run> spark-submit etl.py 2>&1 | grep ETL

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


