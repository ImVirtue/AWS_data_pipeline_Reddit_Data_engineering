import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import concat_ws, col


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = StructType([
    StructField("title", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("url", StringType(), True),
    StructField("edited", StringType(), True),
    StructField("spoiler", StringType(), True),
    StructField("stickied", StringType(), True)
])

AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "escapeChar": "\\"},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://reddit-data-engineering-fmbvirtue/datalake/raw_csv_file/reddit_posts_2025-03-21.csv"],
        "recurse": True
    },
    transformation_ctx="AmazonS3_node"
)

df = AmazonS3_node.toDF().selectExpr(
    "title",
    "cast(score as string) as score",
    "cast(num_comments as string) as num_comments",
    "author",
    "created_utc",
    "url",
    "edited",
    "spoiler",
    "stickied"
)

df_combined = df.withColumn('ESS_updated', concat_ws('-', col('edited'), col('spoiler'), col('stickied')))
df_combined = df_combined.drop('edited', 'spoiler', 'stickied')

S3bucket_node_transformed = DynamicFrame.fromDF(df_combined, glueContext, "S3bucket_node_transformed")

EvaluateDataQuality().process_rows(
    frame=S3bucket_node_transformed,
    ruleset="Rules = [ColumnCount > 0]",
    publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node_transformed,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": "s3://reddit-data-engineering-fmbvirtue/datalake/transformed/",
        "partitionKeys": []
    },
    transformation_ctx="AmazonS3_node_write"
)

job.commit()
