import botocore.exceptions
import configparser
import logging
import boto3
import time

def UpdateConfig(filepath, section, option, value):
    """
    Updates a config file with the passed values.

    Parameters:
    - filepath (str): The absolue filepath of the config file.
    - section (str): The section in the config to update.
    - option (str): The item under the specified section to be updated.
    - value (str): Value to assign the item.
    """
    # Read the config and preserve the casing of items
    config = configparser.ConfigParser(interpolation=None)
    config.optionxform = str # type: ignore
    config.read_file(open(filepath))

    # Update the option with the new value
    config.set(section, option, value)

    # Save the changes back to the config file
    with open(filepath, 'w') as configfile:
        config.write(configfile)

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

def DeleteRole(RoleName):
    """
    Deletes the IAM role with the given name.

    Parameters:
    - RoleName (str): The name of the IAM role.

    Returns:
    - Boolean: True if IAM role is deleted, false otherwise.
    """
    result = False
    iam = boto3.client('iam')
    try:
        # Try to fetch IAM role details
        iam.get_role(RoleName=RoleName)

        # If the IAM role exists, detach policies
        for policy in iam.list_attached_role_policies(RoleName=RoleName)['AttachedPolicies']:
            iam.detach_role_policy(RoleName=RoleName, PolicyArn=policy['PolicyArn'])
            logging.info(f"Detached policy '{policy['PolicyName']}' from IAM Role '{RoleName}'.")

        # After detaching policies, delete the IAM role
        iam.delete_role(RoleName=RoleName)
        logging.info(f"IAM Role '{RoleName}' deleted successfully.")
        success = False
    except botocore.exceptions.ClientError as e:
        # Check if the error code is NoSuchEntity
        if e.response['Error']['Code'] == 'NoSuchEntity':
            # IAM role does not exist, print a message
            logging.error(f"IAM role '{RoleName}' does not exist. Skipping IAM role deletion.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while deleting IAM Role '{RoleName}': {e}")
    
    return result

def FetchClusterProps(ClusterIdentifier):
    """
    Fetches properties of the Redshift cluster with the given identifier.

    Parameters:
    - ClusterIdentifier (str): The identifier of the Redshift cluster.

    Returns:
    - dict or None: Cluster properties if the cluster exists, None otherwise.

    Note: If the cluster does not exist, an error message will be logged.
    """
    redshift = boto3.client('redshift')
    clusterProps = None
    try:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier)['Clusters'][0]
    except botocore.exceptions.ClientError as e:
        # Check if the error code is ClusterNotFound
        if e.response['Error']['Code'] == 'ClusterNotFound':
            # Cluster does not exist, print a message
            logging.error(f"Cluster '{ClusterIdentifier}' does not exist.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while fetching the Redshift Cluster details for {ClusterIdentifier}: {e}")

    return clusterProps

def RevokeIngress(ClusterProps, DbPort):
    """
    Revokes the ingress rule for the security group associated with the Redshift cluster.

    Parameters:
    - ClusterProps (dict): Properties of the Redshift cluster.
    - DbPort (int): The port number to revoke.
    """
    ec2 = boto3.resource('ec2')
    defaultSg = None

    try:
        # Check if the security group exists
        if ClusterProps is None:
            raise Exception('CusterProps must not be None. Skipping Ingress revocation.') 
        vpc = ec2.Vpc(id=ClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
    except Exception as e:
        logging.error(f'Another error occurred: {e}')
        return

    # Undo the authorize_ingress
    try:
        defaultSg.revoke_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='tcp',
            FromPort=int(DbPort),
            ToPort=int(DbPort)
        )
        logging.info("Ingress rule revoked successfully.")
    except botocore.exceptions.ClientError as revoke_error:
        if revoke_error.response['Error']['Code'] == 'InvalidPermission.NotFound':
            logging.warning("Ingress rule not found. Skipping revoke.")
        else:
            logging.error(f"An error occurred during revoke: {revoke_error}")

def DeleteCluster(ClusterIdentifier, TimeoutSeconds, SleepDuration):
    """
    Deletes the Redshift cluster with the given identifier.

    Parameters:
    - ClusterIdentifier (str): The identifier of the Redshift cluster.
    - TimeoutSeconds (int): The timeout afterwhich checking for cluster deletion will terminate (seconds).
    - SleepDuration (int): Duration to sleep between polling for resource status.

    Returns:
    - Boolean: True if cluster is deleted, false otherwise.
    """
    clusterDeleted = False
    redshift = boto3.client('redshift')

    try:
        # Delete Redshift Cluster
        redshift.delete_cluster(ClusterIdentifier=ClusterIdentifier, SkipFinalClusterSnapshot=True)

        # Wait for the cluster to be deleted
        start_time = time.time()

        while not clusterDeleted:
            # Check timeout
            time_elapsed = time.time() - start_time
            if time_elapsed > TimeoutSeconds:
                logging.error(f"Timeout waiting for cluster '{ClusterIdentifier}' to be deleted.")
                break
            try:
                logging.info(f'Cluster is deleting...{time_elapsed} seconds elapsed.')
                redshift.describe_clusters(ClusterIdentifier=ClusterIdentifier) # If cluster is deleted the exception will be triggered
                time.sleep(SleepDuration)
            except botocore.exceptions.ClientError as describe_error:
                if describe_error.response['Error']['Code'] == 'ClusterNotFound':
                    clusterDeleted = True
                else:
                    logging.error(f"An error occurred during describe cluster: {describe_error}")

    except botocore.exceptions.ClientError as e:
        # Check if the error code is ClusterNotFound
        if e.response['Error']['Code'] == 'ClusterNotFound':
            # Cluster does not exist, print a message
            logging.error(f"Cluster '{ClusterIdentifier}' does not exist. Skipping cluster deletion.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while deleting the Redshift Cluster {ClusterIdentifier}: {e}")
    
    return clusterDeleted