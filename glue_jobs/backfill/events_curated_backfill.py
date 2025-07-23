import boto3
import time
from pyspark.sql.functions import col, to_timestamp, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# Paths
raw_path = "s3://wistiabucket/raw/events/"
curated_path = "s3://wistiabucket/curated/events/"

# Athena info
database_name = 'wistia_project'
table_name = 'events'   
athena_results_bucket = 's3://wistiabucket/athena-results/' 

# Step 1: Read all raw JSON event files
df_raw = spark.read.json(raw_path)

# Step 2: Convert event_timestamp to timestamp type
df_ts = df_raw.withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

# Step 3: Extract event_date from event_timestamp for partitioning
df = df_ts.withColumn("event_date", to_date(col("event_timestamp")))

# Step 4: Deduplicate events by keys
deduped_df = df.dropDuplicates(['media_id', 'visitor_id', 'event_key', 'event_timestamp'])

# Step 5: Write deduplicated data to curated layer partitioned by event_date
deduped_df.write.mode("overwrite").partitionBy("event_date").parquet(curated_path)

# Step 6: Register partitions in Athena using boto3

athena_client = boto3.client('athena')

# Get distinct event_date partitions just written
partitions = deduped_df.select("event_date").distinct().collect()
partitions = [row['event_date'].strftime('%Y-%m-%d') for row in partitions]

for part_date in partitions:
    partition_location = f"{curated_path}event_date={part_date}/"
    partition_sql = f"""
    ALTER TABLE {table_name}
    ADD IF NOT EXISTS
    PARTITION (event_date='{part_date}')
    LOCATION '{partition_location}'
    """
    response = athena_client.start_query_execution(
        QueryString=partition_sql,
        QueryExecutionContext={'Database': database_name},
        ResultConfiguration={'OutputLocation': athena_results_bucket}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete (simple polling)
    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state == 'SUCCEEDED':
        print(f"Partition for date {part_date} registered successfully.")
    else:
        print(f"Failed to register partition for date {part_date}: {state}")

job.commit()
