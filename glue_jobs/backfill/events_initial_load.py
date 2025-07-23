import requests
import boto3
import json
import pandas as pd
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# === CONFIG ===
bucket = 'wistiabucket'
prefix = 'raw/events'
secret_name = 'wistia/api_token'
region_name = 'us-east-1'
media_ids = ['gskhw4w4lm', 'v08dlrgr7v']

# === GET API TOKEN FROM SECRETS MANAGER ===
def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret['API_TOKEN']

API_TOKEN = get_secret(secret_name, region_name)
headers = {'Authorization': f'Bearer {API_TOKEN}'}

# === FETCH EVENTS ===
def get_event_data(media_id):
    url = 'https://api.wistia.com/v1/stats/events.json'
    params = {'media_id': media_id, 'per_page': 100, 'page': 1}
    events = []

    while True:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        page = resp.json()

        for event in page:
            ts = event.get('received_at')
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))  # Convert to datetime
            events.append({
                'media_id': media_id,
                'visitor_id': event.get('visitor_key'),
                'ip_address': event.get('ip'),
                'country': event.get('country'),
                'event_key': event.get('event_key'),
                'watched_percent': event.get('percent_viewed'),
                'event_timestamp': ts,
                'event_date': dt.date().isoformat()
            })

        if len(page) < params['per_page']:
            break
        params['page'] += 1

    return events

# === SAVE TO S3 PARTITIONED BY event_date ===
def save_events_to_s3_partitioned(events):
    s3 = boto3.client('s3')
    df = pd.DataFrame(events)

    for event_date, group in df.groupby("event_date"):
        s3_key = f"{prefix}/event_date={event_date}/events.json"
        json_str = group.drop(columns=['event_date']).to_json(orient='records', lines=True)

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json_str.encode('utf-8')
        )
        print(f"Saved {len(group)} events to s3://{bucket}/{s3_key}")

# === MAIN RUNNER ===
def run_bulk_load():
    all_events = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(get_event_data, media_ids))
        for r in results:
            all_events.extend(r)

    save_events_to_s3_partitioned(all_events)

run_bulk_load()
