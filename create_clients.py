import boto3
import configparser

config = configparser.ConfigParser()
config.read("dwh.cfg")

KEY = config["AWS"]["KEY"]
SECRET = config["AWS"]["SECRET"]


def s3Client():
    s3 = boto3.resource(
        "s3",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    return s3


def ec2Client():
    ec2 = boto3.resource(
        "ec2",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    return ec2


def iamClient():
    iam = boto3.client(
        "iam",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name="us-west-2",
    )
    return iam


def redshiftClient():
    redshift = boto3.client(
        "redshift",
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
    )
    return redshift
