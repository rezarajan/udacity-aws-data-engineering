import botocore.exceptions
import configparser
import logging
import boto3

def main():
    # Parse datawarehouse config dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    REGION_NAME            = config.get('AWS', 'REGION_NAME')

    CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")

    IAM_ROLE_NAME      = config.get("IAM", "IAM_ROLE_NAME")

    # Create AWS Resources
    boto3.setup_default_session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION_NAME
    )

    iam = boto3.client('iam')
    redshift = boto3.client('redshift')

    try:
        # Delete Redshift Cluster
        response = redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        # clusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        # clusterStatus = clusterProps.items()['ClusterStatus']
        # assert clusterStatus == 'deleting'
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
        iam_role = iam.get_role(RoleName=IAM_ROLE_NAME)

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