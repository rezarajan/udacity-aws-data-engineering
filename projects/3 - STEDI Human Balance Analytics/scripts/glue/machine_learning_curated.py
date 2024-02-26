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
accelerometer_trusted_node1708926351999 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1708926351999",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1708926352335 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://aws-dend-project-3/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1708926352335",
)

# Script generated for node SQL Query
SqlQuery0 = """
select
    s.serialNumber,
    a.user,
    s.sensorReadingTime,
    s.distanceFromObject,
    a.x, a.y, a.z
from step_trainer_trusted s
join accelerometer_trusted a
    on a.timestamp = s.sensorReadingTime
;
"""
SQLQuery_node1708926354505 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_trusted": step_trainer_trusted_node1708926352335,
        "accelerometer_trusted": accelerometer_trusted_node1708926351999,
    },
    transformation_ctx="SQLQuery_node1708926354505",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1708926562725 = glueContext.getSink(
    path="s3://aws-dend-project-3/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1708926562725",
)
machine_learning_curated_node1708926562725.setCatalogInfo(
    catalogDatabase="stedi-project-3", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1708926562725.setFormat("json")
machine_learning_curated_node1708926562725.writeFrame(SQLQuery_node1708926354505)
job.commit()
