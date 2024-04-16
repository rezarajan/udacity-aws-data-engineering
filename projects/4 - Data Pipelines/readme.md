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

# Dev Notes
### Operators

`StageToRedshiftOperator`: This operator stages data from S3 into a table in Redshift.
- To avoid errors, ensure that the staging table exists in Redshift prior to running.
- The default S3 key is compiled from the execution date in the format of YYYY/MM (e.g. s3://some_bucket/2018/11/). This isn't always useful as you'd likely want to specify an s3 key after the bucket; therefore you have the option to template this parameter as follows: `s3_key = some_key/{{ execution_date.strftime("%Y/%m") }}`, resuling in a path like s3://some_bucket/some_key/2018/11.
- redshift_conn_id and aws_credentials_id have defaults, but it is recommended that these be set anyway.

`LoadFactOperator`: This operator only allows for append-style operations on tables.
- Takes as input the target table and a sql query string.
- Duplicate entries will be ignored, by default; this does not handle any upserts.
