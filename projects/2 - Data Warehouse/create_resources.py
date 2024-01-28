import botocore.exceptions
import configparser
import logging
import boto3
import json
import time
from pathlib import Path
from update_config import UpdateConfig

def CreateRole(RoleName, AssumeRolePolicyDocument):
    """
    Creates an IAM role and attaches 'AmazonS3ReadOnlyAccess' policy.

    Parameters:
    - role_name (str): The name of the IAM role.

    Returns:
    - str: The ARN of the created IAM role, or an empty string if role already exists.

    Example:
    ```
    role_arn = create_role('MyRole')
    print(f'IAM Role ARN: {role_arn}')
    ```

    IAM Policy attached:
    - AmazonS3ReadOnlyAccess
    """
    # Create IAM client
    roleArn = '' # Defaults to a blank string
    iam = boto3.client('iam')
    try:
        # Attempt to create IAM role
        dwhRole = iam.create_role(
            Path='/',
            RoleName=RoleName,
            AssumeRolePolicyDocument=AssumeRolePolicyDocument,
            Description='DWH Role Created using boto3'
        )

        # Attach S3 Read Only Access Policy to the role
        iam.attach_role_policy(
            RoleName=RoleName,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )

        # Fetch role ARN for use with other resources
        roleArn = dwhRole['Role']['Arn']
        logging.info(f'IAM Role Created: ARN {roleArn}')

    except botocore.exceptions.ClientError as e:
        # Check if the error code is EntityAlreadyExists
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            # IAM role already exists, print a message or handle accordingly
            logging.warning(f"IAM role '{RoleName}' already exists. Skipping role creation.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while creating IAM Role '{RoleName}': {e}")

    return roleArn

def CreateCluster(RoleARN, ClusterConfig, TimeoutSeconds, SleepDuration):
    """
    Creates a Redshift cluster and waits for it to become available.

    Parameters:
    - RoleARN (str): The ARN of the IAM role to be associated with the cluster.
    - ClusterConfig (dict): A dictionary containing Redshift cluster configuration parameters.
    - TimeoutSeconds (int): The timeout afterwhich checking for cluster creation will terminate (seconds).
    - SleepDuration (int): The interval at which cluter readiness will be probed (seconds).

    Returns:
    - dict or None: Cluster properties if the cluster is created successfully, None otherwise.

    Example:
    ```
    cluster_config = {
        'ClusterIdentifier': 'example-cluster',
        'ClusterType': 'multi-node',
        'NodeType': 'dc2.large',
        # Add other configuration parameters
    }
    role_arn = 'arn:aws:iam::123456789012:role/RedshiftRole'
    cluster_properties = createCluster(role_arn, cluster_config)
    ```

    Note:
    This function creates a Redshift cluster using boto3 and waits for the cluster to become available.
    """

    # Create redshift client
    redshift = boto3.client('redshift')

    try:
        clusterProps = None
        # Attempt to create Redshift cluster
        redshift.create_cluster(IamRoles=[RoleARN], **ClusterConfig)

        # Wait for the cluster to become available
        clusterReady = False
        start_time = time.time()
        while not clusterReady:
            # Check timeout
            time_elapsed = time.time() - start_time
            if time_elapsed > TimeoutSeconds:
                logging.error(f"Timeout waiting for cluster '{ClusterConfig['ClusterIdentifier']}' to become available.")
                break
            # Get the cluster properties
            _clusterProps = redshift.describe_clusters(ClusterIdentifier=ClusterConfig['ClusterIdentifier'])['Clusters'][0]
            
            if _clusterProps['ClusterStatus'] != 'available':
                logging.info(f'Cluster is creating...{time_elapsed} seconds elapsed.')
                time.sleep(SleepDuration)
                continue

            # Set the cluster properties
            clusterProps = _clusterProps
            clusterReady = True

        return clusterProps

    except botocore.exceptions.ClientError as e:
        # Check if the error code is ClusterAlreadyExists
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            # Redshift cluster already exists, print a message or handle accordingly
            logging.warning(f"Redshift cluster '{ClusterConfig['ClusterIdentifier']}' already exists. Skipping cluster creation.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while creating Redshift cluster '{ClusterConfig['ClusterIdentifier']}': {e}")

def CreateIngress(ClusterProps, DbPort):
    """
    Opens incoming TCP traffic to a Redshift cluster.

    Parameters:
    - ClusterProps (dict): The properties of the Redshift cluster obtained from describe_clusters.
    - DbPort (int): The port number to be opened for incoming TCP traffic.

    Example:
    ```
    cluster_props = {
        'Endpoint': {'Address': 'example-cluster.cxihzodzutpq.us-west-2.redshift.amazonaws.com'},
        'IamRoles': [{'IamRoleArn': 'arn:aws:iam::123456789012:role/RedshiftRole'}],
        'VpcId': 'vpc-0123456789abcdef0',
        # Add other cluster properties
    }
    db_port = 5439
    createIngress(cluster_props, db_port)
    ```

    Note:
    This function opens incoming TCP traffic to the Redshift cluster specified in ClusterProps.
    It uses the VPC, security group, and port information to authorize ingress.
    """

    assert ClusterProps is not None, "ClusterProps must not be None"

    ec2 = boto3.resource('ec2')

    # Fetch cluster info
    ENDPOINT = ClusterProps['Endpoint']['Address']
    ROLE_ARN = ClusterProps['IamRoles'][0]['IamRoleArn']

    # Open incoming TCP to the cluster
    vpc = ec2.Vpc(id=ClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]

    try:
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='tcp',
            FromPort=int(DbPort),
            ToPort=int(DbPort)
        )
    except botocore.exceptions.ClientError as creation_error:
        if creation_error.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            logging.warning("Ingress rule already exists. Skipping.")
        else:
            logging.error(f"An error occurred during creation: {creation_error}")

def main():
    logging.basicConfig(level=logging.INFO)  # Set the logging level

    # Load pararameters from dwh.cfg
    path = Path(__file__)
    ROOT_DIR = path.parent.absolute() # Use root path if calling script from a separate directory
    config_path = Path(ROOT_DIR, 'dwh.cfg')
    config = configparser.ConfigParser()
    config.read_file(open(config_path))

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
    DB_PORT            = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME      = config.get("IAM", "IAM_ROLE_NAME")

    # Timeout settings when waiting for resources to be created
    TIMEOUT_SECONDS = 600
    SLEEP_DURATION = 10

    # Create AWS Resources
    boto3.setup_default_session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION_NAME
    )

    # Define a role policy document which allows Redshift to Assume Role
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
    # Create IAM Role
    roleArn = CreateRole(IAM_ROLE_NAME, assume_role_policy_document)
    UpdateConfig(config_path, 'IAM', 'IAM_ROLE_ARN', roleArn) # Update the config value

    # Create Redshift Cluster
    cluster_config = {
        'DBName': DB_NAME,
        'ClusterIdentifier': CLUSTER_IDENTIFIER,
        'ClusterType': CLUSTER_TYPE,
        'NodeType': NODE_TYPE,
        'MasterUsername': DB_USER,
        'MasterUserPassword': DB_PASSWORD,
        'Port': int(DB_PORT),
        'NumberOfNodes': int(NUM_NODES)
    }

    clusterProps = CreateCluster(roleArn, cluster_config, TIMEOUT_SECONDS, SLEEP_DURATION)
    if clusterProps:
        # Update config with endpoint
        UpdateConfig(config_path, 'CLUSTER', 'ENDPOINT', clusterProps['Endpoint']['Address'])
    
    # Create Ingress
    CreateIngress(clusterProps, DB_PORT)


if __name__ == '__main__':
    main()