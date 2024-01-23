import botocore.exceptions
import configparser
import logging
import boto3
import time
from pathlib import Path

def main():
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

    # Create AWS Resources
    boto3.setup_default_session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION_NAME
    )

    ec2 = boto3.resource('ec2')
    iam = boto3.client('iam')
    redshift = boto3.client('redshift')

    try:
        # Fetch existing cluster properties for the VPC ID later
        clusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

        # Check if the security group exists
        vpc = ec2.Vpc(id=clusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        # Undo the authorize_ingress
        try:
            defaultSg.revoke_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='tcp',
                FromPort=int(DB_PORT),
                ToPort=int(DB_PORT)
            )
            logging.info("Ingress rule revoked successfully.")
        except botocore.exceptions.ClientError as revoke_error:
            if revoke_error.response['Error']['Code'] == 'InvalidPermission.NotFound':
                logging.warning("Ingress rule not found. Skipping revoke.")
            else:
                logging.error(f"An error occurred during revoke: {revoke_error}")

        # Delete Redshift Cluster
        redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)

        # Wait for the cluster to be deleted
        # Set timeout and sleep duration
        timeout_seconds = 600  # Adjust as needed
        sleep_duration = 10
        clusterDeleted = False
        start_time = time.time()

        while not clusterDeleted:
            # Check timeout
            time_elapsed = time.time() - start_time
            if time_elapsed > timeout_seconds:
                logging.error(f"Timeout waiting for cluster '{CLUSTER_IDENTIFIER}' to be deleted.")
                break
            try:
                logging.info(f'Cluster is deleting...{time_elapsed} seconds elapsed.')
                redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)
                time.sleep(sleep_duration)
            except botocore.exceptions.ClientError as describe_error:
                if describe_error.response['Error']['Code'] == 'ClusterNotFound':
                    clusterDeleted = True
                else:
                    logging.error(f"An error occurred during describe cluster: {describe_error}")


    except botocore.exceptions.ClientError as e:
        # Check if the error code is ClusterNotFound
        if e.response['Error']['Code'] == 'ClusterNotFound':
            # Cluster does not exist, print a message
            logging.error(f"Cluster '{CLUSTER_IDENTIFIER}' does not exist. Skipping cluster deletion.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while deleting the Redshift Cluster {CLUSTER_IDENTIFIER}: {e}")
    
    try:
        # Try to fetch IAM role details
        iam.get_role(RoleName=IAM_ROLE_NAME)

        # If the IAM role exists, detach policies
        for policy in iam.list_attached_role_policies(RoleName=IAM_ROLE_NAME)['AttachedPolicies']:
            iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn=policy['PolicyArn'])
            print(f"Detached policy '{policy['PolicyName']}' from IAM Role '{IAM_ROLE_NAME}'.")

        # After detaching policies, delete the IAM role
        iam.delete_role(RoleName=IAM_ROLE_NAME)
        logging.info(f"IAM Role '{IAM_ROLE_NAME}' deleted successfully.")
    except botocore.exceptions.ClientError as e:
        # Check if the error code is NoSuchEntity
        if e.response['Error']['Code'] == 'NoSuchEntity':
            # IAM role does not exist, print a message
            logging.error(f"IAM role '{IAM_ROLE_NAME}' does not exist. Skipping IAM role deletion.")
        else:
            # Handle other errors
            logging.error(f"An error occurred while deleting IAM Role '{IAM_ROLE_NAME}': {e}")

if __name__ == '__main__':
    main()