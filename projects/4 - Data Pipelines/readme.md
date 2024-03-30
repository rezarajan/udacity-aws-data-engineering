# Project Overview

# Procedure
## 1. Upload raw data to s3

First, the raw data must be uploaded to S3. A script has been created to perform the necessary actions. Issue the following command from the project's root directory: 
```sh 
sh scripts/upload_data_to_s3.sh
```

*Note:* 
  - The following variables should be changed in the script:
    - `S3_BUCKET` changes the target bucket
    - `PREFIX` changes the path within the bucket. A blank value will default to the root of the bucket.
  - For this project the bucket `aws-dend-airflow` is created and the project data files are copied over.


