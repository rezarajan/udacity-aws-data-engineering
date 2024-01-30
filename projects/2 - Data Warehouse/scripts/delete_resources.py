import logging
import boto3
from helpers import LoadConfig, UpdateConfig, DeleteRole, FetchClusterProps, RevokeIngress, DeleteCluster

def main():
    logging.basicConfig(level=logging.INFO)  # Set the logging level
    
    # Autoload dwh config file
    config = LoadConfig(autoload=True)

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

    # Delete the Redshift Cluster and update the config
    clusterDeleted = DeleteCluster(CLUSTER_IDENTIFIER, TIMEOUT_SECONDS, SLEEP_DURATION)
    if clusterDeleted:
        UpdateConfig('CLUSTER', 'HOST', '', autoload=True)
    
    # Delete the IAM Role and update the config
    roleDeleted = DeleteRole(IAM_ROLE_NAME)
    if clusterDeleted:
        UpdateConfig('IAM', 'IAM_ROLE_ARN', '', autoload=True)


if __name__ == '__main__':
    main()