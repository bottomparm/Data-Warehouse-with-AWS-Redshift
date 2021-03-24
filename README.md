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

2. Run the `aws_config.py` file.

NOTE: You'll want to do this once, and wait 5-10 minutes for your Redshift Cluster to be created, and then run it again to programmatically update your config file.

3. Once you can run `aws_config.py` and only receive errors about resources already existing, you will have an AWS Redshift Cluster with a DB and table schema.

4. Logging into your AWS portal in the browser, you can use the Query Editor in the Redshift console to check your DB.

