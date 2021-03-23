import boto3
import configparser

config = configparser.ConfigParser()
config.read("dwh.cfg")

KEY = config["AWS"]["KEY"]
SECRET = config["AWS"]["SECRET"]


def s3Client():
    s3 = boto3.resource(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    return s3
