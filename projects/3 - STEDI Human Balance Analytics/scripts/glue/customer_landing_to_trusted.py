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

# Script generated for node customer_landing
customer_landing_node1708868072189 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1708868072189",
)

# Script generated for node filter_consent
SqlQuery0 = """
select * from customer_landing
where shareWithResearchAsOfDate != 0;
"""
filter_consent_node1708868177245 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": customer_landing_node1708868072189},
    transformation_ctx="filter_consent_node1708868177245",
)

# Script generated for node customer_trusted
customer_trusted_node1708868089932 = glueContext.getSink(
    path="s3://aws-dend-project-3/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1708868089932",
)
customer_trusted_node1708868089932.setCatalogInfo(
    catalogDatabase="stedi-project-3", catalogTableName="customer_trusted"
)
customer_trusted_node1708868089932.setFormat("json")
customer_trusted_node1708868089932.writeFrame(filter_consent_node1708868177245)
job.commit()
