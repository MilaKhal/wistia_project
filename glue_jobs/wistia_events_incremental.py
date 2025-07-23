import boto3
import requests
import json
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
import sys
import os
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from py4j.protocol import Py4JJavaError

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job_name = args['JOB_NAME']
except Exception:
    job_name = 'default_job'
    args = {'JOB_NAME': job_name}

# Initialize the job
job.init(job_name, args)

# ---- Step 1: Load API token ----
def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret['API_TOKEN']

API_TOKEN = get_secret("wistia/api_token")
headers = {"Authorization": f"Bearer {API_TOKEN}"}

# ---- Step 2: Set date range (yesterday) ----
yesterday = (datetime.now() - timedelta(days=1)).date()
start_date = end_date = str(yesterday)

# ---- Step 3: Get event data ----
media_ids = ['gskhw4w4lm', 'v08dlrgr7v']
all_events = []

def get_event_data(media_id):
    url = 'https://api.wistia.com/v1/stats/events.json'
    params = {
        'media_id': media_id,
        'start_date': start_date,
        'end_date': end_date,
        'per_page': 100,
        'page': 1
    }
    events = []

    while True:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        page = resp.json()

        for event in page:
            events.append({
                'media_id': media_id,
                'visitor_id': event.get('visitor_key'),
                'ip_address': event.get('ip'),
                'country': event.get('country'),
                'event_key': event.get('event_key'),
                'watched_percent': event.get('percent_viewed'),
                'event_timestamp': event.get('received_at'),
                'event_date': start_date  # for partitioning
            })

        if len(page) < params['per_page']:
            break
        params['page'] += 1

    return events

# Fetch all
try:
    for media_id in media_ids:
        all_events.extend(get_event_data(media_id))
except requests.RequestException as e:
    print(f"❌ API error for Wistia events: {e}")
    job.commit()
    sys.exit(1)

# ---- Step 4: Save to S3 ----
if all_events:
    df = spark.read.json(sc.parallelize([json.dumps(e) for e in all_events]))
    s3_path = f"s3://wistiabucket/raw/events/event_date={start_date}/"
    df.write.mode("overwrite").json(s3_path)

    # ---- Step 5: Process raw -> curated (dedup, transform, parquet) ----
    try:
        raw_df = spark.read.json(f"s3://wistiabucket/raw/events/event_date={start_date}/")
        dedup_df = raw_df.dropDuplicates(["visitor_id", "media_id", "event_timestamp"])
    
        curated_path = f"s3://wistiabucket/curated/events/"
        dedup_df.write.mode("overwrite").partitionBy("event_date").parquet(curated_path)
        print(f"✅ Wrote curated data to: {curated_path}")
    except Py4JJavaError as e:
        print(f"❌ Error in transformation step: {e}")
    
    # ---- Step 6: Register new curated partitions in Athena ----
    athena_client = boto3.client('athena')
    
    partition_sql = f"""
    ALTER TABLE events
    ADD IF NOT EXISTS
    PARTITION (event_date = '{start_date}')
    LOCATION 's3://wistiabucket/curated/events/event_date={start_date}/'
    """
    
    athena_client.start_query_execution(
        QueryString=partition_sql,
        QueryExecutionContext={
            'Database': 'wistia_project'
        },
        ResultConfiguration={
            'OutputLocation': 's3://wistiabucket/athena-results/'
        }
    )

else:
    print("No new events for this date.")
    
job.commit()