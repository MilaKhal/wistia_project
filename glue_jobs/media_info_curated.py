from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import input_file_name, to_timestamp
import sys
import time
import logging

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def with_retry(func, retries=3, delay=10):
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempt {attempt}...")
            return func()
        except Exception as e:
            logger.error(f"Error on attempt {attempt}: {str(e)}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logger.error("All retry attempts failed.")
                raise

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    logger.info("Glue job started.")

    # Read all raw media info JSONs with retry
    def read_data():
        logger.info("Reading raw media_info JSON files from S3...")
        df = spark.read.option("multiline", "true").json("s3://wistiabucket/raw/media_info/*/*.json")
        df = df.withColumn("created_at", to_timestamp("created_at"))
        df = df.withColumn("updated_at", to_timestamp("updated_at"))
        return df

    df = with_retry(read_data)

    # Write curated output with retry
    def write_data():
        logger.info("Writing curated data to S3 as Parquet...")
        df.write \
            .mode("overwrite") \
            .partitionBy("media_id") \
            .parquet("s3://wistiabucket/curated/media_info/")
        logger.info("Write successful.")

    with_retry(write_data)

    logger.info("Glue job completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as final_error:
        logger.error(f"Glue job failed: {str(final_error)}")
        raise
