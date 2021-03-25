import configparser
import json
import pandas as pd
from create_clients import s3Client, ec2Client, iamClient, redshiftClient
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_IAM_ROLE_NAME = config["IAM_ROLE"]["NAME"]
DWH_DB = config["CLUSTER"]["DB_NAME"]
DWH_DB_USER = config["CLUSTER"]["DB_USER"]
DWH_DB_PASSWORD = config["CLUSTER"]["DB_PASSWORD"]
DWH_CLUSTER_IDENTIFIER = config["CLUSTER"]["CLUSTER_IDENTIFIER"]
DWH_CLUSTER_TYPE = config["CLUSTER"]["CLUSTER_TYPE"]
DWH_NUM_NODES = config["CLUSTER"]["NUM_NODES"]
DWH_NODE_TYPE = config["CLUSTER"]["NODE_TYPE"]
DWH_PORT = config["CLUSTER"]["DB_PORT"]

"""Establish clients for AWS services"""
s3 = s3Client()
redshift = redshiftClient()
iam = iamClient()
ec2 = ec2Client()


def readS3Data():
    """Print data from S3 to console"""
    sampleDbBucket = s3.Bucket("udacity-dend")
    # for obj in sampleDbBucket.objects.filter(Prefix="log_data"):
    #     print(obj)
    # for obj in sampleDbBucket.objects.all():
    #     print(obj)


def createIamRole():
    """Create an IAM Role with S3ReadOnlyAccess permissions"""
    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    print(roleArn)
    return roleArn


def createRedshiftCluster(roleArn):
    """Create Redshift Cluster with params from dwh.cfg"""
    try:
        response = redshift.create_cluster(
            # Cluster info
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
    except Exception as e:
        print(e)


def redshiftProps(props):
    """Create a pandas dataframe of the cluster info and print it to console"""
    pd.set_option("display.max_colwidth", None)
    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    print(pd.DataFrame(data=x, columns=["Key", "Value"]))


def openTcpPort(myClusterProps):
    """Open a TCP port to allow connections to the cluster"""
    try:
        vpc = ec2.Vpc(id=myClusterProps["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)


def cleanup():
    """grab redshift props from current cluster if running"""
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    """delete cluster"""
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )
    """delete IAM role"""
    iam.detach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    """print cluster status to console"""
    redshiftProps(myClusterProps)


def updateConfig(endpoint, roleArn):
    """Update the dwh.cfg file with host and port values"""
    host = endpoint["Address"]
    port = endpoint["Port"]

    config["ENDPOINT"] = {"host": host, "port": port}
    config["IAM_ROLE"] = {"name": DWH_IAM_ROLE_NAME, "roleArn": roleArn}
    # writing to configuration file
    with open("dwh.cfg", "w") as configfile:
        config.write(configfile)


def main():
    readS3Data()
    roleArn = createIamRole()
    createRedshiftCluster(roleArn)
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]
    DWH_ENDPOINT = myClusterProps["Endpoint"]["Address"]
    DWH_ROLE_ARN = myClusterProps["IamRoles"][0]["IamRoleArn"]
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    redshiftProps(myClusterProps)
    openTcpPort(myClusterProps)

    """if redshift cluster is available and config has not been updated, update config"""
    if myClusterProps["Endpoint"] and not config.has_section("ENDPOINT"):
        updateConfig(myClusterProps["Endpoint"], DWH_ROLE_ARN)

    """Uncomment to run cleanup func and delete AWS redshift cluster and iam role"""
    # cleanup()


if __name__ == "__main__":
    main()
