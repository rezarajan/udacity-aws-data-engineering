import configparser
import logging
import boto3
from pathlib import Path
from helpers import UpdateConfig, DeleteRole, FetchClusterProps, RevokeIngress, DeleteCluster

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

    # Delete the Redshift Cluster and update the config
    clusterDeleted = DeleteCluster(CLUSTER_IDENTIFIER, TIMEOUT_SECONDS, SLEEP_DURATION)
    if clusterDeleted:
        UpdateConfig(config_path, 'CLUSTER', 'HOST', '')
    
    # Delete the IAM Role and update the config
    roleDeleted = DeleteRole(IAM_ROLE_NAME)
    if clusterDeleted:
        UpdateConfig(config_path, 'IAM', 'IAM_ROLE_ARN', '')


if __name__ == '__main__':
    main()