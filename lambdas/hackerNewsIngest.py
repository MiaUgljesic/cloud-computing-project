import json
import urllib.request
import boto3
import os
import datetime

s3 = boto3.client('s3')
# Extract the bucket name from the configured environment variables
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")


"""Helper function to fetch a single item from the Hacker News API"""
def fetch_item(item_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None

def lambda_handler(event, context):
    print("[INFO] Starting Hacker News data ingestion pipeline...")
    
    try:
        endpoints = {
            "story": "https://hacker-news.firebaseio.com/v0/newstories.json",
            "ask": "https://hacker-news.firebaseio.com/v0/askstories.json",
            "job": "https://hacker-news.firebaseio.com/v0/jobstories.json",
            "show": "https://hacker-news.firebaseio.com/v0/showstories.json"
        }
        
        fetched_items = []
        comment_ids = []
        
        # 1. Fetch all post types (limited to 15 per type to prevent Lambda Timeout)
        for post_type, url in endpoints.items():
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                ids = json.loads(response.read().decode())
                
            for item_id in ids[:15]:
                item = fetch_item(item_id)
                if item:
                    fetched_items.append(item)
                    # Collect kids (comments)
                    if "kids" in item:
                        comment_ids.extend(item["kids"][:2])

        # 2. Fetch the actual comment items
        for cid in comment_ids[:20]:
            comment = fetch_item(cid)
            if comment:
                fetched_items.append(comment)

        # 3. Upload raw data to S3 (Bronze Layer)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"hacker_news/raw_stories_{timestamp}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(fetched_items, indent=4)
        )
        
        success_message = f"[SUCCESS] Successfully ingested {len(fetched_items)} HN objects (stories, asks, jobs, comments)."
        print(success_message)
        
        return {
            'statusCode': 200,
            'body': {
                'message': success_message,
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        print(f"[ERROR] Ingestion failed: {str(e)}")
        raise e