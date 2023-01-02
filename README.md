# Data lake - AWS with Spark and S3

The project have the objective of creating a data lake for the user activity and song metadata located in the S3 in a json format. 

We will have to build an ETL pipeline that read the data, transform it with spark and load them in S3 in a parquet format. The design of the tables will be a faxt and dimension table that will be usefult for sparkify to analyse their data effectivelly. 


# How to run

First create an IAM role with the Policy AmazonS3FullAcces.
Then add the information of the  AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the file dl.cfg.

```
AWS_ACCESS_KEY_ID=''
AWS_SECRET_ACCESS_KEY=''
```

Then proceed to create a spark engine that you will run the script. We used an EMR from AWS.

Third, create a S3 bucket where you will store the parquet files. and add in the main section in the etl.py in output data with the '/' in the end.
```py
output_data = "s3a://sparkify-dend-example/"
```

Finally you can run the ETL using the spark engine.




# Tables Design
The design of the Tables follow the star schema where we have a fact table that hold the information that is important to analyse and keep track of the insightful information. Then, we will have 4 dimensions tables that hold information about single entities that are relevant of the fact table. This way we can easly query the fact table with useful information.

### songplays (Fact table)

Columns:
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### users (Dimension table)

Columns:
user_id, first_name, last_name, gender, level

### songs (Dimension table)

Columns:
song_id, title, artist_id, year, duration

### artists (Dimension table)

Columns:
artist_id, name, location, latitude, longitude

### time (Dimension table)

Columns:
start_time, hour, day, week, month, year, weekday