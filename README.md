# Project 5: Data Pipeline

Objective: Build a data pipeline for a music app (Sparkify) using Airflow and AWS. Two datasets used are both in JSON format, and key parts are extracted from the datasets to build a star schema optimized for queries on song play analysis. 

## Data pipeline structure

### Dags

![sparkify_dag](https://github.com/ohjang121/project_five_data_pipeline/blob/main/sparkify_dag.png)

`sparkify_dag.py`: defines Sparkify data pipeline DAG with relevant tasks and dependencies. Exists in the `dags` directory within the main `airflow` directory.

`create_tables.sql`: contains SQL queries to create staging and production tables in Redshift. `sparkify_dag.py` is currently designed to take `create_tables.sql` as an input in the same directory and create empty shell tables all at once.

### Helpers

`sql_queries.py`: contains SQL queries to perform ETL. Used in conjunction with `load_fact.py` and `load_dimension.py` in `sparkify_dag.py` to insert data into the production tables.

### Operators

`stage_redshift.py`: contains `StageToRedshiftOperator` class that copies data from S3 to the staging tables in Redshift.

`load_fact.py`: contains `LoadFactOperator` class that inserts data from staging tables to the fact table.

`load_dimension.py`: contains `LoadDimensionOperator` class that inserts data from staging tables and fact table to the dimension tables.

`data_quality.py`: contains `DataQualityOperator` class that checks data quality for the data output. 2 main checks are row count and not null checks for primary keys / not null columns. Fails if unexpected output is returned with a value error.

## Airflow Connections

2 connections used were AWS (conn_id = `aws_credentials`) and Redshift (conn_id = `redshift`). Both should be configured in the Airflow UI for the program to work properly.

## Schema & Table Design

The project's star schema contains 1 fact table and 4 dimension tables:

**Fact Table(s)**

1. songplays - records in log data associated with song plays i.e. records with page NextSong
    * columns: songplay_id (pkey), start_time (foreign key to time), user_id (foreign key to users), level, song_id (foreign key to songs), artist_id (foreign key to artists), session_id, location, user_agent, year, month

**Dimension Table(s)**
1. users - users in the app
    * columns: userid (pkey), first_name, last_name, gender, level
2. songs - songs in music database
    * columns: songid (pkey), title, artist_id, year, duration
3. artists - artists in music database
    * columns: artistid (pkey), name, location, latitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    * columns: start_time (pkey), hour, day, week, month, year, weekday

## Helpful notes:

* any issues with operator logic (syntax, import, etc.) leads to import error in the main dag
* must run airflow scheduler prior to airflow webserver -p 8080 in order to see most recent script changes
* install additional packages for hooks & connections to work (e.g. apache-airflow-providers-postgres or apache-airflow-providers-amazon)
* make sure the S3 bucket & Redshift cluster are in the same region
* enable inbound rules to be connected via local in the chosen VPC in the redshift cluster
