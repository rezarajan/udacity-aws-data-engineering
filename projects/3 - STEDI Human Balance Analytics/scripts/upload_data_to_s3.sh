#!/bin/bash

# Variables
S3_BUCKET=aws-dend-project-3
GIT_REPO=https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git
TEMP_DIR=/tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises

# Clone the Git repository
git clone $GIT_REPO $TEMP_DIR

# Sync the contents of the project/starter folder with the S3 bucket
aws s3 sync $TEMP_DIR/project/starter s3://$S3_BUCKET --exclude "README.md"

# Clean up temporary directory
rm -rf $TEMP_DIR

echo "Content uploaded to S3 bucket: $S3_BUCKET"
