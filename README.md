## Project: Data Lake

### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals. 

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

I tested the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare your results with their expected results.

### State and justify your database schema design and ETL pipeline.

#### Schema for Song Play Analysis
Using the song and log datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table
* songplays - records in log data associated with song plays i.e. records with page NextSong
** songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
* users - users in the app: user_id, first_name, last_name, gender, level
* songs - songs in music database: song_id, title, artist_id, year, duration
* artists - artists in music database: artist_id, name, location, lattitude, longitude
* time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday

### Project Template
The project template includes three files:

* etl.py reads data from S3, processes that data using Spark, and writes them back to S3
* dl.cf gcontains AWS credentials
* README.md provides discussion on your process and decisions