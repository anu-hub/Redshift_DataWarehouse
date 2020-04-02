**Project Summary:**

Sparkify a music start up wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.
As the data is growing they want to move the data warehouse from on-premis to AWS Redshift (Cloud Data Warehouse).
Data storage will be in S3 and the type of file is JSON.

This project involves building an ETL pipeline by creating Redshit clusters to extract data from S3 and load into the staging tables and after required transformations load them into the fact & dimension tables.


**Project Implementation Steps:**

### 1. Created Redshift Cluster with the following configuration. Created an IAM Role and attched the role to the cluster, also created a user and security group.

```CLUSTER_TYPE=multi-node```
```NUM_NODES=4```
```NODE_TYPE=dc2.large```

### 2. Data Modeling: 

Database: dwhsparkify
Schema: sparkify

Staging Tables
#### 1. stg_events - Loaded from S3 (JSON log files)
#### 2. stg_songs - Loaded from S3 (JSON song files)

Fact Table: songplays (songplay_id pk)
Fact table is loaded from the stg_events & stg_songs tables joing artist name, song duration and song title.  

```Artist_Id``` & ```Song_Id``` - These columns are populated from stg_songs staging table.
Start_Time,User_Id,Level,Session_ID,Location & user_agent - These columns are populated from stg_events staging table.


Dimension Tables: users(user_id pk), songs(song_id pk), artists(artist_id pk), time(start_time pk)

User & Time Dimenensions are populated from stg_events staging table 
Songs & Artist dimentions are populated from stg_songs staging table

### 3. ETL Flow

We are using three scripts for the ETL process and a configuration File

Configuration File: Stores the database details, cluster details, ARN Role information

sql_queries: table creation, table drop, insert & select query formats are storted in this script, which is being imported in the etl.py scripy and create_table.py script.

```create_tables.py``` : This script should be executed before the etl script, it will create the database, staging tables and the analytics tables (fact & dimension).

ETL script is used to load the staging tables from the song & log json files located in the AWS S3 storage. Analytics tables are loaded from the staging tables after the transformations.


**Steps to execute the scripts:**

### 1. Execte create_tables.py script - Script to creates the database, staging, fact & dimension tables, also drops the tables if exists

### 2. sql_queries: No action required - This script is imported in create_table & etl script.

**Functionality:**
Create table queries - Fact is created with primary key, distribution key & sort key. Dimension tables are created with primary key and also have the sort keys.
Drop table Queries
Copy to stagging table from S3 commands - Copy command has been implemented with required parameter and optional parameter for error handling.

Insert queries to load Fact & Dimension tables - Null handling check in the select queries from staging tables.

### 3. Execute etl.py script  - This will load, transform and insert the data into the tables

Main function calls the two other functions: load_staging_tables & insert_tables

```load_staging_tables``` - staging table load using COPY commands from AWS S3 storage
```insert_tables``` - analytics table load from the staging tables - stg_events & stg_songs




