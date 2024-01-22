import configparser
import boto3
import json

def main():
    # Parse datawarehouse config dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    REGION_NAME            = config.get('AWS', 'REGION_NAME')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    # Create AWS Resources
    boto3.setup_default_session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION_NAME
    )

    ec2 = boto3.resource('ec2')
    s3 = boto3.resource('s3')
    iam = boto3.client('iam')
    redshift = boto3.client('redshift')

    # Create IAM Role
    # Allows Redshift to Assume Role
    assume_role_policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    })

    try:
        dwhRole = iam.create_role(
                    Path='/',
                    RoleName=DWH_IAM_ROLE_NAME,
                    AssumeRolePolicyDocument=assume_role_policy_document,
                    Description='DWH Role Created using boto3'
                )
    except Exception as e:
        print(e)

    # Attach S3 Read Only Access Policy to the role
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    # Fetch role ARN for use with other resources
    roleArn = iam.get_role(
        RoleName=DWH_IAM_ROLE_NAME
    )['Role']['Arn']

    # Create Redshift Cluster
    try:
        response = redshift.create_cluster(
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            Port=int(DWH_PORT),
            NumberOfNodes=int(DWH_NUM_NODES),
            IamRoles=[
                roleArn
            ]
        )
    except Exception as e:
        print(e)

    # Wait for the cluster to become available
    clusterReady = False
    clusterProps = None
    while not clusterReady:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        clusterStatus = clusterProps.items()['ClusterStatus']
        if not clusterStatus == 'available':
            continue
        if clusterStatus == 'available':
            clusterReady = True
            break

    if clusterProps is not None:
        # Fetch cluster info
        DWH_ENDPOINT = clusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']

        # Open incoming TCP to the cluster
        try:
            vpc = ec2.Vpc(id=clusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='tcp',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
        except Exception as e:
            print(e)

if __name__ == '__main__':
    main()