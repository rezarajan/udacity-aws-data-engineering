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

# Script generated for node customer_curated
customer_curated_node1708884813753 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1708884813753",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1708884813456 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1708884813456",
)

# Script generated for node SQL Query
SqlQuery0 = """
select
    s.*,
    case when 
        s.sensorReadingTime >= c.shareWithResearchAsOfDate then 0
        else 1
    end as piiexclude
from step_trainer_landing s
join customer_curated c
    on c.serialNumber = s.serialNumber
;
"""
SQLQuery_node1708884818899 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_curated": customer_curated_node1708884813753,
        "step_trainer_landing": step_trainer_landing_node1708884813456,
    },
    transformation_ctx="SQLQuery_node1708884818899",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1708884822010 = glueContext.getSink(
    path="s3://aws-dend-project-3/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1708884822010",
)
step_trainer_trusted_node1708884822010.setCatalogInfo(
    catalogDatabase="stedi-project-3", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1708884822010.setFormat("json")
step_trainer_trusted_node1708884822010.writeFrame(SQLQuery_node1708884818899)
job.commit()
