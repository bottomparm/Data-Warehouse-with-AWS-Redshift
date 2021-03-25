# README

## Questions for Reviewer

1. I am having trouble with the `ts` column in the log_data set. I am trying to cast it into a timestamp and have tried several methods but redshift doesn't seem to like any of them. Do I start with it in `BIGINT` form?

2. I had to remove the `PRIMARY KEY` designation from `user_id` in the `users` table because I kept receiving an error saying that I could not insert a null value for `user_id`. I'm assuming it was referring to the `user_id` in the `users` table.

## Overview

Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project constructs an ETL pipeline that extracts data from S3, stages it in Redshift, and transforms data into a set of dimensional tables for an analytics team to continue finding insights in what songs their users are listening to.

Project Description

1. Build an ETL pipeline for a database hosted on Redshift.
2. Load data from S3 to staging tables on Redshift.
3. Execute SQL statements that create the analytics tables from these staging tables.

## Schema Design

For this project I elected to go with a Star Schema database design. I did this to provide simplicitity and speed for SQL queries.
## Installation

Use a package manager (pip) to install the following packages:

```bash
pip install configparser
pip install psycopg2
pip install psycopg2-binary
pip install pandas
pip install boto3
```

## Usage

1. You'll need to create a dwh.cfg file with the following information:

```config
[AWS]
KEY=''
SECRET=''

[CLUSTER]
DB_NAME=''
DB_USER=''
DB_PASSWORD=''
DB_PORT=''
CLUSTER_IDENTIFIER=''
CLUSTER_TYPE=''
NUM_NODES=''
NODE_TYPE=''

[IAM_ROLE]
NAME=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

2. Run the `aws_config.py` file.

NOTE: You'll want to do this once, and wait 5-10 minutes for your Redshift Cluster to be created, and then run it again to programmatically update your config file.

3. Once you can run `aws_config.py` and only receive errors about resources already existing, you will have an AWS Redshift Cluster with a DB and table schema.

4. Logging into your AWS portal in the browser, you can use the Query Editor in the Redshift console to check your DB.

