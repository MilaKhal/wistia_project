import sys
import boto3
import json
import requests
from awsglue.utils import getResolvedOptions
from dateutil.parser import parse as parse_datetime

# Get secret from Secrets Manager
def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret['API_TOKEN']  
    
# Define channel for each media
def get_channel_from_media_id(media_id):
    if media_id == "gskhw4w4lm":
        return "YouTube"
    elif media_id == "v08dlrgr7v":
        return "Facebook"
    else:
        return "Unknown"

# Read existing file (if any)        
def fetch_existing_json_from_s3(media_id):
    s3_key = f"raw/media_info/media_id={media_id}/{media_id}.json"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"Error fetching existing S3 object for {media_id}: {e}")
        return None
# Fetch API data
def fetch_media_data(media_id):
    media_url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    response = requests.get(media_url, headers=headers)
    
    if response.status_code == 200:
        media_info = response.json()
        name = media_info.get("name", "")
        duration = media_info.get("duration")
        created_at = media_info.get("created", "")
        updated_at = media_info.get("updated", "")
        id = media_info.get("id")
        channel = get_channel_from_media_id(media_id)
        
        # The media URL is usually in the assets list, pick the first asset url
        assets = media_info.get("assets", [])
        url = ""
        if assets:
            url = assets[0].get("url", "")
            if url.endswith(".bin"):
                url = url[:-4] + ".mp4"
        
        return {
            "media_id": media_id,
            "id": id,
            "name": name,
            "url": url,
            "channel": channel,
            "duration": duration,
            "created_at": created_at,
            "updated_at": updated_at
        }
    else:
        print(f"Failed to fetch media {media_id}: {response.status_code}")
        return None

# Upload to respective folder
def upload_json_to_s3(media_id, media_info):
    s3_key = f"raw/media_info/media_id={media_id}/{media_id}.json"
    json_str = json.dumps(media_info, indent=2)
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_str,
            ContentType='application/json'
        )
        print(f"Uploaded JSON for {media_id} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload JSON for {media_id}: {e}")


# Set up
bucket_name = 'wistiabucket'
secret_name = 'wistia/api_token'  # or whatever your secret name is
region_name = 'us-east-1'

API_TOKEN = get_secret(secret_name, region_name)
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}
s3 = boto3.client('s3')

media_id_to_channel = {
    "gskhw4w4lm": "YouTube",
    "v08dlrgr7v": "Facebook"
}
# Run the job
media_list = [
    {"media_id": "gskhw4w4lm", "channel": "YouTube"},
    {"media_id": "v08dlrgr7v", "channel": "Facebook"}
]
for media in media_list:
    media_id = media['media_id']
    latest_info = fetch_media_data(media_id)
    if not latest_info:
        continue  # skip if API fetch failed
    
    existing_info = fetch_existing_json_from_s3(media_id)
    if existing_info:
        # Compare timestamps
        latest_updated = parse_datetime(latest_info['updated_at'])
        existing_updated = parse_datetime(existing_info['updated_at'])

        if latest_updated <= existing_updated:
            print(f"No update needed for {media_id} (already up to date).")
            continue
        upload_json_to_s3(media_id, latest_info)
