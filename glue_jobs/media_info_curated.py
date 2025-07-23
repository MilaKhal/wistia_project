from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import input_file_name
import sys
from pyspark.sql.functions import to_timestamp
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read all raw media info JSONs
df = spark.read.option("multiline", "true").json("s3://wistiabucket/raw/media_info/*/*.json")
df = df.withColumn("created_at", to_timestamp("created_at"))
df = df.withColumn("updated_at", to_timestamp("updated_at"))
# Write to curated layer in Parquet, partitioned by media_id
df.write \
  .mode("overwrite") \
  .partitionBy("media_id") \
  .parquet("s3://wistiabucket/curated/media_info/")
