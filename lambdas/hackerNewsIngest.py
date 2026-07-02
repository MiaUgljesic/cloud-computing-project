import datetime
import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
import boto3

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
HN_ENDPOINTS = {
    "story": f"{HN_BASE_URL}/newstories.json",
    "ask": f"{HN_BASE_URL}/askstories.json",
    "job": f"{HN_BASE_URL}/jobstories.json",
    "show": f"{HN_BASE_URL}/showstories.json"
}


def _http_request(url: str) -> bytes | None:
    """Helper function to perform HTTP GET requests safely with a custom User-Agent."""
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req) as response:
            return response.read()
    except Exception as e:
        logger.warning(f"HTTP request failed for URL {url}: {str(e)}")
        return None


def _fetch_item(item_id: int | str) -> dict | None:
    """Fetches a single item (story, comment, etc.) from the Hacker News API."""
    url = f"{HN_BASE_URL}/item/{item_id}.json"
    response_data = _http_request(url)
    
    if response_data:
        try:
            return json.loads(response_data.decode())
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON for item ID: {item_id}")
    return None


def lambda_handler(event, context):
    raise Exception("TEST_GRESKA: Testiramo Discord notifikacije!") 
    logger.info("Starting Hacker News data ingestion pipeline...")
    
    try:
        fetched_items = []
        comment_ids = []
        
        # 1. Fetch item IDs from all endpoints (limited to 15 per type to prevent Lambda Timeout)
        for post_type, url in HN_ENDPOINTS.items():
            logger.info(f"Fetching latest IDs for type: {post_type}")
            endpoint_data = _http_request(url)
            
            if not endpoint_data:
                continue
                
            try:
                item_ids = json.loads(endpoint_data.decode())
            except json.JSONDecodeError:
                logger.error(f"Failed to decode endpoint list for {post_type}")
                continue
            
            # Fetch details for the first 15 items
            for item_id in item_ids[:15]:
                item = _fetch_item(item_id)
                if item:
                    fetched_items.append(item)
                    # Safely collect children (comments) - top 2 per story
                    if "kids" in item:
                        comment_ids.extend(item["kids"][:2])

        # 2. Fetch the actual comment items (capped at 20 overall)
        logger.info(f"Collected {len(comment_ids)} total potential comments. Fetching top 20...")
        for cid in comment_ids[:20]:
            comment = _fetch_item(cid)
            if comment:
                fetched_items.append(comment)

        # 3. Upload aggregated raw data to S3 (Bronze Layer)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"hacker_news/raw_stories_{timestamp}.json"
        
        logger.info(f"Uploading {len(fetched_items)} objects to S3 bucket: {BUCKET_NAME}")
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(fetched_items, indent=4)
        )
        
        success_msg = f"Successfully ingested {len(fetched_items)} HN objects (stories, asks, jobs, comments)."
        logger.info(success_msg)
        
        return {
            'statusCode': 200,
            'body': {
                'message': success_msg,
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        raise e