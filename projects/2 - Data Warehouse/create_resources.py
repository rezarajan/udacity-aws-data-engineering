import botocore.exceptions
import configparser
import logging
import boto3
import json
import time

def main():
    logging.basicConfig(level=logging.INFO)  # Set the logging level
    # Parse datawarehouse config dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                = config.get('AWS','KEY')
    SECRET             = config.get('AWS','SECRET')
    REGION_NAME        = config.get('AWS', 'REGION_NAME')

    CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
    NUM_NODES          = config.get("CLUSTER","NUM_NODES")
    NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")

    CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB_NAME            = config.get("CLUSTER","DB_NAME")
    DB_USER            = config.get("CLUSTER","DB_USER")
    DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT               = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME      = config.get("IAM", "IAM_ROLE_NAME")

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

    roleArn = '' # Placeholder for IAM role ARN

    try:
        # Attempt to create IAM role
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=assume_role_policy_document,
            Description='DWH Role Created using boto3'
        )

        # Attach S3 Read Only Access Policy to the role
        iam.attach_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )

        # Fetch role ARN for use with other resources
        roleArn = dwhRole['Role']['Arn']
        logging.info(f'IAM Role Created: ARN {roleArn}')

    except botocore.exceptions.ClientError as e:
        # Check if the error code is EntityAlreadyExists
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # IAM role already exists, print a message or handle accordingly
            logging.warning(f"IAM role '{IAM_ROLE_NAME}' already exists. Skipping role creation.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while creating IAM Role '{IAM_ROLE_NAME}': {e}")


    try:
        clusterProps = None
        if roleArn != '':
            # Attempt to create Redshift cluster
            redshift.create_cluster(
                DBName=DB_NAME,
                ClusterIdentifier=CLUSTER_IDENTIFIER,
                ClusterType=CLUSTER_TYPE,
                NodeType=NODE_TYPE,
                MasterUsername=DB_USER,
                MasterUserPassword=DB_PASSWORD,
                Port=int(DB_PORT),
                NumberOfNodes=int(NUM_NODES),
                IamRoles=[roleArn]
            )

            # Wait for the cluster to become available
            # Set timeout and sleep duration
            timeout_seconds = 600
            sleep_duration = 10
            clusterReady = False
            start_time = time.time()
            while not clusterReady:
                # Check timeout
                time_elapsed = time.time() - start_time
                if time_elapsed > timeout_seconds:
                    logging.error(f"Timeout waiting for cluster '{CLUSTER_IDENTIFIER}' to become available.")
                    break

                clusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
                clusterStatus = clusterProps['ClusterStatus']
                
                if clusterStatus != 'available':
                    logging.info(f'Cluster is creating...{time_elapsed} seconds elapsed.')
                    time.sleep(sleep_duration)
                    continue
                
                clusterReady = True

        if clusterProps is not None:
            # Fetch cluster info
            ENDPOINT = clusterProps['Endpoint']['Address']
            ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']

            # Open incoming TCP to the cluster
            vpc = ec2.Vpc(id=clusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]

            try:
                defaultSg.authorize_ingress(
                    GroupName=defaultSg.group_name,
                    CidrIp='0.0.0.0/0',
                    IpProtocol='tcp',
                    FromPort=int(DB_PORT),
                    ToPort=int(DB_PORT)
                )
            except botocore.exceptions.ClientError as creation_error:
                if creation_error.response['Error']['Code'] == 'InvalidPermission.Duplicate':
                    logging.warning("Ingress rule already exists. Skipping.")
                else:
                    logging.error(f"An error occurred during creation: {creation_error}")

    except botocore.exceptions.ClientError as e:
        # Check if the error code is ClusterAlreadyExists
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            # Redshift cluster already exists, print a message or handle accordingly
            logging.warning(f"Redshift cluster '{CLUSTER_IDENTIFIER}' already exists. Skipping cluster creation.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while creating Redshift cluster '{CLUSTER_IDENTIFIER}': {e}")


if __name__ == '__main__':
    main()