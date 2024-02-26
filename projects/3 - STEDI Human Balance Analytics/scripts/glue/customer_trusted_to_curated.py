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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1708922896238 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/accelerometer/trusted"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1708922896238",
)

# Script generated for node customer_trusted
customer_trusted_node1708922896679 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1708922896679",
)

# Script generated for node inner_join
SqlQuery0 = """
select c.* from customer_trusted c
join accelerometer_trusted a
    on lower(a.user) = lower(c.email)
;
"""
inner_join_node1708922899326 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1708922896679,
        "accelerometer_trusted": accelerometer_trusted_node1708922896238,
    },
    transformation_ctx="inner_join_node1708922899326",
)

# Script generated for node customer_curated
customer_curated_node1708922901442 = glueContext.getSink(
    path="s3://aws-dend-project-3/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1708922901442",
)
customer_curated_node1708922901442.setCatalogInfo(
    catalogDatabase="stedi-project-3", catalogTableName="customer_curated"
)
customer_curated_node1708922901442.setFormat("glueparquet", compression="snappy")
customer_curated_node1708922901442.writeFrame(inner_join_node1708922899326)
job.commit()
