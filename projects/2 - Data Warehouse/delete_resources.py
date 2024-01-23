import botocore.exceptions
import configparser
import logging
import boto3
import time
from pathlib import Path

def DeleteRole(RoleName):
    """
    Deletes the IAM role with the given name.

    Parameters:
    - RoleName (str): The name of the IAM role.
    """
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
    except botocore.exceptions.ClientError as e:
        # Check if the error code is NoSuchEntity
        if e.response['Error']['Code'] == 'NoSuchEntity':
            # IAM role does not exist, print a message
            logging.error(f"IAM role '{RoleName}' does not exist. Skipping IAM role deletion.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while deleting IAM Role '{RoleName}': {e}")

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
    """
    redshift = boto3.client('redshift')

    try:
        # Delete Redshift Cluster
        redshift.delete_cluster(ClusterIdentifier=ClusterIdentifier, SkipFinalClusterSnapshot=True)

        # Wait for the cluster to be deleted
        clusterDeleted = False
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

def main():
    logging.basicConfig(level=logging.INFO)  # Set the logging level
    
    # Load pararameters from dwh.cfg
    path = Path(__file__)
    ROOT_DIR = path.parent.absolute() # Use root path if calling script from a separate directory
    config_path = Path(ROOT_DIR, 'dwh.cfg')
    config = configparser.ConfigParser()
    config.read_file(open(config_path))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    REGION_NAME            = config.get('AWS', 'REGION_NAME')

    CLUSTER_IDENTIFIER     = config.get("CLUSTER","CLUSTER_IDENTIFIER")
    DB_PORT                = config.get("CLUSTER","DB_PORT")

    IAM_ROLE_NAME          = config.get("IAM", "IAM_ROLE_NAME")

    # Timeout settings when waiting for resources to be created
    TIMEOUT_SECONDS = 600
    SLEEP_DURATION = 10

    # Create AWS Resources
    boto3.setup_default_session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION_NAME
    )

    clusterProps = FetchClusterProps(CLUSTER_IDENTIFIER)
    # Revoke Ingress Rules
    RevokeIngress(clusterProps, DB_PORT)
    # Delete the Redshift Cluster
    DeleteCluster(CLUSTER_IDENTIFIER, TIMEOUT_SECONDS, SLEEP_DURATION)
    # Delete the IAM Role
    DeleteRole(IAM_ROLE_NAME)


if __name__ == '__main__':
    main()