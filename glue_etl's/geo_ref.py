import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Code hereunder pulls geographically enriched csv file deposited to S3 from Sagemaker
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://alternate-pty-buck/sfl/georef/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Code hereunder deposits csv file to S3 folder feeding geographic reference table in athena
AmazonS3_node1650245902618 = glueContext.getSink(
    path="s3://alternate-pty-buck/sfl/geo_ref/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1650245902618",
)
AmazonS3_node1650245902618.setCatalogInfo(
    catalogDatabase="sfl", catalogTableName="geo_ref"
)
AmazonS3_node1650245902618.setFormat("glueparquet")
AmazonS3_node1650245902618.writeFrame(S3bucket_node1)
job.commit()
