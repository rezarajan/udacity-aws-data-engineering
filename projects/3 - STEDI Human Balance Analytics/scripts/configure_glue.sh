#!/bin/bash

# Script to configure IAM and routes for Glue

# Usage: ./configure_glue.sh [-f|--force]
#    -f, --force   Force deletion of existing role

# Variables
GLUE_ROLE_NAME=glue-service-role-project-3
S3_BUCKET=aws-dend-project-3

# Policy Documents
ASSUME_ROLE_POLICY_DOCUMENT=$(jq -n '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}' | jq -c '.')

S3_POLICY_DOCUMENT=$(jq -n --arg bucket "$S3_BUCKET" '{
    Version: "2012-10-17",
    Statement: [
        {
            Sid: "ListObjectsInBucket",
            Effect: "Allow",
            Action: [
                "s3:ListBucket"
            ],
            Resource: [
                "arn:aws:s3:::" + $bucket
            ]
        },
        {
            Sid: "AllObjectActions",
            Effect: "Allow",
            Action: "s3:*Object",
            Resource: [
                "arn:aws:s3:::" + $bucket + "/*"
            ]
        }
    ]
}' | jq -c '.')

DEFAULT_GLUE_POLICY_DOCUMENT=$(jq -n '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}' | jq -c '.')

# Fetch the default VPC and route table IDs
VPC_ID=$(aws ec2 describe-vpcs | jq -r '.Vpcs[].VpcId')
ROUTE_TABLE_ID=$(aws ec2 describe-route-tables | jq -r '.RouteTables[].RouteTableId')

# Create a VPC endpoint
aws ec2 create-vpc-endpoint \
    --vpc-id $VPC_ID \
    --service-name com.amazonaws.us-west-2.s3 \
    --route-table-ids $ROUTE_TABLE_ID \
    --no-cli-pager

# Check if the role exists and force flag is not set
if [ "$(aws iam get-role --role-name $GLUE_ROLE_NAME --no-cli-pager 2>/dev/null)" != "" ] && [ "$1" != "-f" -a "$1" != "--force" ]; then
    echo "Role $GLUE_ROLE_NAME already exists. Use -f or --force to delete the existing role."
    exit 1
fi

# If the role exists and the force flag is set, delete the role
if [ "$(aws iam get-role --role-name $GLUE_ROLE_NAME --no-cli-pager 2>/dev/null)" != "" ] && [ "$1" = "-f" -o "$1" = "--force" ]; then
    # Delete all attached policies
    attached_policies=$(aws iam list-attached-role-policies --role-name $GLUE_ROLE_NAME --no-cli-pager | jq -r '.AttachedPolicies[].PolicyArn')
    for policy_arn in $attached_policies; do
        aws iam detach-role-policy --role-name $GLUE_ROLE_NAME --policy-arn $policy_arn --no-cli-pager
    done

    # Delete all inline policies
    inline_policies=$(aws iam list-role-policies --role-name $GLUE_ROLE_NAME --no-cli-pager | jq -r '.PolicyNames[]')
    for policy_name in $inline_policies; do
        aws iam delete-role-policy --role-name $GLUE_ROLE_NAME --policy-name $policy_name --no-cli-pager
    done

    # Delete the role
    aws iam delete-role --role-name $GLUE_ROLE_NAME --no-cli-pager
fi

# Create a Glue Servie Role
aws iam create-role --role-name $GLUE_ROLE_NAME --assume-role-policy-document "$ASSUME_ROLE_POLICY_DOCUMENT" --no-cli-pager 2>/dev/null

# Grant Glue Privileges on the S3 Bucket
aws iam put-role-policy --role-name $GLUE_ROLE_NAME --policy-name S3Access --policy-document "$S3_POLICY_DOCUMENT" --no-cli-pager 2>/dev/null

# Grant Glue General Access
aws iam put-role-policy --role-name $GLUE_ROLE_NAME --policy-name GlueAccess --policy-document $DEFAULT_GLUE_POLICY_DOCUMENT --no-cli-pager 2>/dev/null
