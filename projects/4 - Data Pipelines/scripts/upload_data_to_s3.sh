#!/bin/bash

# Variables
S3_BUCKET=aws-dend-airflow
PREFIX=project-4
TEMP_DIR=/tmp/$PREFIX

# -- DO NOT MODIFY BELOW ---
# Set defaults
#S3_BUCKET
if [ -z "${S3_BUCKET+x}" ]; then
    echo "Error: S3_BUCKET variable is not set. Please set the S3_BUCKET variable before running this script."
    exit 1
fi

# PREFIX
if [ -z "${PREFIX+x}" ]; then
    PREFIX=""
    echo "No prefix set, using root of S3 bucket"
elif [ "$PREFIX" != "" ] && [ "${PREFIX: -1}" != "/" ]; then
    PREFIX="${PREFIX}/"
fi

# Copy the files to a local directory
mkdir -p $TEMP_DIR
aws s3 cp s3://udacity-dend/log-data/ $TEMP_DIR/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ $TEMP_DIR/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json $TEMP_DIR/ 

# Sync the contents of the project/starter folder with the S3 bucket
aws s3 sync $TEMP_DIR s3://${S3_BUCKET}/${PREFIX}

# Verify the data is available in the destination bucket
aws s3 ls s3://${S3_BUCKET}/${PREFIX}log-data/
aws s3 ls s3://${S3_BUCKET}/${PREFIX}song-data/
aws s3 ls s3://${S3_BUCKET}/${PREFIX}log_json_path.json

# Clean up temporary directory
rm -rf $TEMP_DIR

echo "Content uploaded to S3 bucket: ${S3_BUCKET}/${PREFIX}"
