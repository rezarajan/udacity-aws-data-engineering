import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1708876796557 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1708876796557",
)

# Script generated for node customer_trusted
customer_trusted_node1708876796930 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1708876796930",
)

# Script generated for node SQL Query
SqlQuery0 = """
select 
    a.*,
    case when 
        a.timestamp >= c.shareWithResearchAsOfDate then 0
        else 1
    end as piiexclude
from customer_trusted c
join accelerometer_landing a
    on lower(a.user) = lower(c.email)
;
"""
SQLQuery_node1708876799784 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1708876796930,
        "accelerometer_landing": accelerometer_landing_node1708876796557,
    },
    transformation_ctx="SQLQuery_node1708876799784",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1708876806884 = glueContext.getSink(
    path="s3://aws-dend-project-3/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1708876806884",
)
accelerometer_trusted_node1708876806884.setCatalogInfo(
    catalogDatabase="stedi-project-3", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1708876806884.setFormat("json")
accelerometer_trusted_node1708876806884.writeFrame(SQLQuery_node1708876799784)
job.commit()
