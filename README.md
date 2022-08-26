# Sparkify: ETL Data Warehouse On Redshift

### Introduction

- A music streaming startup, Sparkify want to move their processes and data onto the cloud. 

- The data resides initially on S3, in a directory of JSON logs on user activity and events ( like user_id, start_time ... ) , as well as a directory with JSON metadata on the songs in their app ( like song_id, artist_id ... ).

### Project Description

- The task is building an ETL pipeline that 
    - extracts the data from S3
    - stages it in Redshift
    - transforms data into a set of dimensional tables so the analytics team can continue finding insights into what songs their users are listening to.

### Data 
- we will be working with two datasets that reside in S3. Here are the S3 links for each:
    - **Song data**: s3://udacity-dend/song_data
    - **Log data**: s3://udacity-dend/log_data


### Database Schema

- The database schema is a star schema that includes the following tables.
    - **Fact Table**
        - songplays - records in event data associated with song plays i.e. records with page NextSong 
            - **column_names**: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - **Dimension Tables**
        - **users** - users in the app
            - **column_names**: user_id, first_name, last_name, gender, level
        - **songs** - songs in music database
            - **column_names**: song_id, title, artist_id, year, duration
        - **artists** - artists in music database
            - **column_names**: artist_id, name, location, lattitude, longitude
        - **time** - timestamps of records in songplays broken down into specific units
            - **column_names**: start_time, hour, day, week, month, year, weekday


### How to run the Python scripts
To run the python scripts:
1. open the terminal 
2. type **python create_tables.py**
3. hit enter 
4. type **python etl.py**
5. hit enter 



### An explanation of the files in the repository 
- The repository includes four files:

    - **create_table.py** : This script will drop tables (if exist)  and creates fact and dimension tables for the star schema in Redshift.
    
    - **etl.py** : load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
    
    - **sql_queries.py** : where all the SQL statements lives, which will be imported into the two other files above.
    
    - **README.md**
    
    - **dhw.cfg** : Configuration file used that contains info about CLUSTER, IAM_ROLE and S3
