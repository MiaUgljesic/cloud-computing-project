import json
import urllib.request
import boto3
import os
import datetime

# Initialize the S3 client
# LocalStack will automatically intercept this inside the container runtime
s3 = boto3.client('s3')

BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

def lambda_handler(event, context):
    print("[INFO] Starting Hacker News data ingestion pipeline...")
    
    try:
        # 1. Fetch the latest top stories from the public Hacker News API
        api_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
        req = urllib.request.Request(api_url, headers={'User-Agent': 'Mozilla/5.0'})
        
        with urllib.request.urlopen(req) as response:
            top_stories_ids = json.loads(response.read().decode())
        
        # Take the top 5 stories for testing purposes
        target_ids = top_stories_ids[:5]
        fetched_items = []
        
        # 2. Loop through each story ID and fetch its detailed payload
        for item_id in target_ids:
            item_url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
            item_req = urllib.request.Request(item_url, headers={'User-Agent': 'Mozilla/5.0'})
            
            with urllib.request.urlopen(item_req) as item_response:
                item_data = json.loads(item_response.read().decode())
                if item_data:
                    fetched_items.append(item_data)
        
        # 3. Create a unique, time-stamped key name for the Bronze Layer storage
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"hacker_news/raw_stories_{timestamp}.json"
        
        # 4. Upload the raw JSON data directly to the S3 Bronze Bucket
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(fetched_items, indent=4)
        )
        
        success_message = f"[SUCCESS] Successfully ingested {len(fetched_items)} items into s3://{BUCKET_NAME}/{s3_key}"
        print(success_message)
        
        # POPRAVKA: Prosleđujemo čist rečnik (objekat) za body, a ne JSON string!
        return {
            'statusCode': 200,
            'body': {
                'message': success_message,
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        error_message = f"[ERROR] Ingestion failed: {str(e)}"
        print(error_message)
        # Force the function to fail so the State Machine catches it
        raise e