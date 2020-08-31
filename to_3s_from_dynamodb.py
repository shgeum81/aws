import sys
import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)

table = glueContext.create_dynamic_frame.from_options(
  connection_type="dynamodb",
  connection_options={
    "dynamodb.input.tableName": "테이블명",
    "dynamodb.throughput.read.percent": "0.25",
    "dynamodb.splits": "100"
  }
)

glueContext.write_dynamic_frame.from_options(
  frame=table,
  connection_type="s3",
  connection_options={
    "path": "s3://경로"
  },
  format="parquet",
  transformation_ctx="datasink"
)
