import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Code hereunder pulls 'raw' customer data from S3
AmazonS3_node1650226840127 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://alternate-pty-buck/sfl/2022/04/17/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1650226840127",
)

# Code hereunder aggregates customer data by gender
Aggregate_node1650226869147 = sparkAggregate(
    glueContext,
    parentFrame=AmazonS3_node1650226840127,
    groups=["gender"],
    aggs=[["id", "countDistinct"]],
    transformation_ctx="Aggregate_node1650226869147",
)

# Code hereunder creates parquet files that feed diversity table in athena
AmazonS3_node1650226905151 = glueContext.write_dynamic_frame.from_options(
    frame=Aggregate_node1650226869147,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://alternate-pty-buck/sfl/SALES/diversity/",
        "partitionKeys": [],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="AmazonS3_node1650226905151",
)

job.commit()
