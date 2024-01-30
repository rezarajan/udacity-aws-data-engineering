import logging
import boto3
import json
from helpers import LoadConfig, UpdateConfig, CreateRole, CreateCluster, CreateIngress

def main():
    logging.basicConfig(level=logging.INFO)  # Set the logging level

    # Autoload dwh config file
    config = LoadConfig(autoload=True)

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
    UpdateConfig('IAM', 'IAM_ROLE_ARN', roleArn, autoload=True) # Update the config value

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
        UpdateConfig('CLUSTER', 'HOST', clusterProps['Endpoint']['Address'], autoload=True)
    
    # Create Ingress
    CreateIngress(clusterProps, DB_PORT)


if __name__ == '__main__':
    main()