import sys
import boto3
import json
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from awsglue.utils import getResolvedOptions
from dateutil.parser import parse as parse_datetime

# === Setup Logging ===
logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger()

# === Get secret from Secrets Manager ===
def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret['API_TOKEN']
    except Exception as e:
        logger.error(f"Failed to retrieve secret: {e}")
        raise

# === Define channel for each media ===
def get_channel_from_media_id(media_id):
    return {
        "gskhw4w4lm": "YouTube",
        "v08dlrgr7v": "Facebook"
    }.get(media_id, "Unknown")

# === Read existing file (if any) ===
def fetch_existing_json_from_s3(media_id):
    s3_key = f"raw/media_info/media_id={media_id}/{media_id}.json"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        logger.info(f"No existing file for {media_id}")
        return None
    except Exception as e:
        logger.warning(f"Error fetching existing S3 object for {media_id}: {e}")
        return None

# === Retry for API calls ===
@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def fetch_media_data(media_id):
    media_url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    response = requests.get(media_url, headers=headers, timeout=10)

    if response.status_code == 200:
        media_info = response.json()
        name = media_info.get("name", "")
        duration = media_info.get("duration")
        created_at = media_info.get("created", "")
        updated_at = media_info.get("updated", "")
        id = media_info.get("id")
        channel = get_channel_from_media_id(media_id)

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
        logger.warning(f"Failed to fetch media {media_id}, status: {response.status_code}")
        raise requests.exceptions.RequestException(f"API failed: {response.status_code}")

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
        logger.info(f"Uploaded JSON for {media_id} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload JSON for {media_id}: {e}")
        raise

# === Set up ===
bucket_name = 'wistiabucket'
secret_name = 'wistia/api_token'
region_name = 'us-east-1'

try:
    API_TOKEN = get_secret(secret_name, region_name)
except Exception:
    logger.critical("Exiting due to failure retrieving API token")
    sys.exit(1)

headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}
s3 = boto3.client('s3')

media_list = [
    {"media_id": "gskhw4w4lm", "channel": "YouTube"},
    {"media_id": "v08dlrgr7v", "channel": "Facebook"}
]

# === Main loop ===
for media in media_list:
    media_id = media['media_id']
    try:
        latest_info = fetch_media_data(media_id)
    except Exception as e:
        logger.error(f"Skipping media {media_id} due to fetch error: {e}")
        continue

    existing_info = fetch_existing_json_from_s3(media_id)
    if existing_info:
        try:
            latest_updated = parse_datetime(latest_info['updated_at'])
            existing_updated = parse_datetime(existing_info['updated_at'])
            if latest_updated <= existing_updated:
                logger.info(f"No update needed for {media_id} (already up to date).")
                continue
        except Exception as e:
            logger.warning(f"Failed to compare timestamps for {media_id}: {e}")

    try:
        upload_json_to_s3(media_id, latest_info)
    except Exception:
        logger.error(f"Upload failed for {media_id}")
