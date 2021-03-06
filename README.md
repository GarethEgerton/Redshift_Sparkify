# Redshift Sparkify

## Summary

Data for a music streaming startup resides in S3. The S3 directory contains JSON logs of user activity as well as JSON metadata of the songs on the app.

* An ETL pipeline was built that extracts Sparkify data from S3, stages it in Redshift and transforms the data into a set of dimensional tables for Sparkify's analytics team to run queries and analyse the data.


### Song data:
* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

This is a subset of the Million Song Dataset: http://millionsongdataset.com/ \
Stored in folders of the form:
* song_data/A/B/C/TRABCEI128F424C983.json
* song_data/A/A/B/TRAABJL12903CDCF1A.json

### Log data:
Generated by an event simulator: https://github.com/Interana/eventsim \
Stored in folders of the form:
* log_data/2018/11/2018-11-12-events.json
* log_data/2018/11/2018-11-13-events.json

### Star Schema
Final Tables created as follows:

***Fact Table***
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
* songplay_id
* start_time
* user_id
* level
* song_id
* artist_id
* session_id
* location
* user_agent

***Dimension Tables***

2. **users** - users in the app
* user_id 
* first_name 
* last_name
* gender 
* level
3. **songs** - songs in music database
* song_id 
* title 
* artist_id 
* year 
* duration
4. **artists** - artists in music database
* artist_id
* name 
* location  
* lattitude
* longitude
5. **time** - timestamps of records in songplays broken down into specific units
* start_time 
* hour 
* day 
* week 
* month 
* year 
* weekday
  
### Files
* create_tables.py
* etl.py
* sql_queries.py
* dwh.cfg (excluded from repo for security)

### Instructions

**Launch an AWS Redhift cluster with 4 nodes**
* Ensure that cluster endpoint is public
* Make sure IAM Role is correctly set up with access to S3

**From terminal run:** 

        create_tables.py

This creates 2 staging tables and 5 final tables

        etl.py

Uses the Redshift copy command to load data from S3 to the staging tables:
* staging_events
* staging_songs

It then tranforms the data from the staging tables and loads it into the final five tables of the star schema.







