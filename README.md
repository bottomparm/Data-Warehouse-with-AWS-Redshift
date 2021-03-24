# README

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

2. Run the create_tables.py file.

NOTE: You'll want to do this once, and wait 5-10 minutes for your Redshift Cluster to be created before running again.

3. Once you can run create_tables.py and the only errors you get are errors explaining you've already created certain resources, you will have an AWS Redshift Cluster with a DB and table schema.

4. Logging into your AWS account, you can use the Query Editor in the Redshift console to check your DB.

