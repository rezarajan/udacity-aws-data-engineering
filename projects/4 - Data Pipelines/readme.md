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

## 2. Set up Redshift User
For the purposes of this project, a new user is created with access to the `prod` database only. First, create and connect to the `prod` database, then execute the following queries in Redshift:

```sql
-- Create a user. Ensure to change the password
CREATE USER prod_svc_user PASSWORD 'someP@ssw0rd';

-- Grant all privileges to the user, in the context of the prod database.
-- This is required for certain dag/task operations.
GRANT ALL ON ALL TABLES IN SCHEMA public TO prod_svc_user;
```
## 3. Deploy Airflow
A docker compose file is provided to quickly spin-up an Airflow instance. Enter the project's directory, and run `docker compose up -d`. Once deployed, the application can be accessed at `localhost:8080`.

## 4. Add connections to Airflow
Under 'Admin -> Connections' add the relevant entries for:
 - AWS CLI access, and 
 - Redshift access with the previously created user.

*Note:* Connection testing is enabled in the docker compose file; be sure to test the connections before saving them.

# Dev Notes
### Operators

`StageToRedshiftOperator`: This operator stages data from S3 into a table in Redshift.
- To avoid errors, ensure that the staging table exists in Redshift prior to running.
- The default S3 key is compiled from the execution date in the format of YYYY/MM (e.g. s3://some_bucket/2018/11/). This isn't always useful as you'd likely want to specify an s3 key after the bucket; therefore you have the option to template this parameter as follows: `s3_key = some_key/{{ execution_date.strftime("%Y/%m") }}`, resuling in a path like s3://some_bucket/some_key/2018/11.
- redshift_conn_id and aws_credentials_id have defaults, but it is recommended that these be set anyway.

`LoadFactOperator`: This operator only allows for append-style operations on tables.
- Takes as input the target table and a sql query string.
- Duplicate entries will be ignored, by default; this does not handle any upserts.

`LoadDimensionOperator`: This operator handles dimension table operations. Notably, it differs from the fact table operators by allowing either truncate-insert (default) or append-style operations on the dimension table.
- Takes as input the target table and a sql query string.
- Switch between truncate-insert and append-style by setting the `truncate` flag to `True` or `False`, respectively.
