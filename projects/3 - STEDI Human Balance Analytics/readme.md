# Project Overview

# Procedure

## 1. Upload Raw Data to S3

First, the raw data must be uploaded to S3, into landing zones. A script is created to do this automatically via the aws cli.

Issue the following command from the project's root directory: 
```sh 
sh scripts/upload_data_to_s3.sh
```

*Note:* The target bucket may be changed by modifying the `S3_BUCKET` variable in the script.

<details>
  <summary>Output</summary>

    
    Cloning into '/tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises'...
    remote: Enumerating objects: 1828, done.
    remote: Counting objects: 100% (182/182), done.
    remote: Compressing objects: 100% (105/105), done.
    remote: Total 1828 (delta 82), reused 141 (delta 67), pack-reused 1646
    Receiving objects: 100% (1828/1828), 30.80 MiB | 8.57 MiB/s, done.
    Resolving deltas: 100% (1431/1431), done.
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348232031.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348232031.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/customer/landing/customer-1691348231425.json to s3://aws-dend-project-3/customer/landing/customer-1691348231425.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231724.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231724.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231881.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231881.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231445.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231445.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231810.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231810.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231931.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231931.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231983.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231983.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231495.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231495.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/accelerometer/landing/accelerometer-1691348231576.json to s3://aws-dend-project-3/accelerometer/landing/accelerometer-1691348231576.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/step_trainer/landing/step_trainer-1691348232038.json to s3://aws-dend-project-3/step_trainer/landing/step_trainer-1691348232038.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/step_trainer/landing/step_trainer-1691348232085.json to s3://aws-dend-project-3/step_trainer/landing/step_trainer-1691348232085.json
    upload: ../../../../tmp/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter/step_trainer/landing/step_trainer-1691348232132.json to s3://aws-dend-project-3/step_trainer/landing/step_trainer-1691348232132.json
    Content uploaded to S3 bucket: aws-dend-project-3
    
</details>

## 2. Create a Database to Store Tables
Before creating tables which will allow us to interact with the data, a database must first be created. Using the aws cli tool, we create a database named `stedi-project-3`

```sh
aws glue create-database --database-input '{
        "Name": "stedi-project-3",
        "Description": "STEDI Data for AWS DEND Project 3."
    }'
```

Landing tables may now be created as follows:

```sh
# Create the customer_landing table
aws athena start-query-execution \
    --query-string file://scripts/athena/customer_landing.sql \
    --query-execution-context Database="stedi-project-3" \
    --result-configuration OutputLocation="s3://aws-dend-project-3/athena/"

# Create the accelerometer_landing table
aws athena start-query-execution \
    --query-string file://scripts/athena/accelerometer_landing.sql \
    --query-execution-context Database="stedi-project-3" \
    --result-configuration OutputLocation="s3://aws-dend-project-3/athena/"
```

### Sample Queries

<!-- ![customer_landing](images/customer_landing.png 'Customer Landing')
![accelerometer_landing](images/accelerometer_landing.png 'Accelerometer Landing') -->

<figure>
  <img src="images/customer_landing.png" alt="Customer Landing">
  <figcaption>Querying the Customer Landing Data</figcaption>
</figure>

<figure>
  <img src="images/accelerometer_landing.png" alt="Accelerometer Landing">
  <figcaption>Querying the Accelerometer Landing Data</figcaption>
</figure>

Of note is that the customer birthdays seems to be abnormal, with years like 1399. However, this error seems systematic, and as noted in a project post, this should not affect the results upstream.