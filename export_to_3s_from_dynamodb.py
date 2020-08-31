import sys
import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

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

table = table.apply_mapping([
('원본필드명','원본데이터형','수정필드명','수정데이터형')
])

glueContext.write_dynamic_frame.from_options(
  frame=table,
  connection_type="s3",
  connection_options={
    "path": "s3://경로"
  },
  format="parquet",
  transformation_ctx="datasink"
)
