import configparser
import json
from create_clients import s3Client, ec2Client, iamClient, redshiftClient
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read("dwh.cfg")

DWH_IAM_ROLE_NAME = config["IAM_ROLE"]["NAME"]

def readS3Data():
    s3 = s3Client()
    sampleDbBucket = s3.Bucket("udacity-dend")
    # for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    #     print(obj)
    for obj in sampleDbBucket.objects.all():
        print(obj)


def createIamRole():
    iam = iamClient()
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                  'Effect': 'Allow',
                  'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)



def main():
    # readS3Data()
    createIamRole()

if __name__ == "__main__":
    main()