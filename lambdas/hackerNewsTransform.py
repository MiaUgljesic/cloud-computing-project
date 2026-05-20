import json
import boto3
import os
import re

# Initialize S3 client
s3 = boto3.client('s3')

BRONZE_BUCKET = "bronze-layer"
SILVER_BUCKET = "silver-layer"

def clean_html(text):
    """Simple utility to remove basic HTML tags from text fields (especially comments)"""
    if not text:
        return ""
    clean_re = re.compile('<.*?>')
    return re.sub(clean_re, '', text)

def lambda_handler(event, context):
    print("[INFO] Starting Hacker News data transformation and normalization...")
    
    # 1. Get the bucket name and file key from the incoming event
    # This allows the function to be triggered automatically when a new file arrives in Bronze
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
    except KeyError:
        # Fallback for manual testing if no S3 event context is provided
        print("[WARN] No S3 event context found. Looking for the latest file manually...")
        bucket = BRONZE_BUCKET
        # List objects in bronze layer to find the most recent one
        objects = s3.list_objects_v2(Bucket=bucket, Prefix="hacker_news/")
        if 'Contents' not in objects:
            return {'statusCode': 404, 'body': json.dumps("No raw data found in Bronze Layer.")}
        # Sort by last modified to get the newest file
        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)
        key = sorted_objects[0]['Key']

    print(f"[INFO] Fetching raw data from s3://{bucket}/{key}")
    
    try:
        # 2. Read the raw JSON file from the Bronze Layer
        response = s3.get_object(Bucket=bucket, Key=key)
        raw_content = response['Body'].read().decode('utf-8')
        raw_items = json.loads(raw_content)
        
        # Dictionary to hold categorized items before uploading
        categorized_data = {
            "story": [],
            "comment": [],
            "job": [],
            "poll": [],
            "ask": []
        }
        
        # 3. Process, clean, and normalize each item
        for item in raw_items:
            item_type = item.get("type", "unknown")
            
            # Normalize fields to guarantee structure
            cleaned_item = {
                "id": item.get("id"),
                "by": item.get("by", "anonymous"),
                "time": item.get("time"),
                "score": item.get("score", 0),
                "title": item.get("title", ""),
                "text": clean_html(item.get("text", "")),
                "url": item.get("url", "")
            }
            
            # Special separation for 'ask' stories (Hacker News marks Asks as stories, but with text)
            if item_type == "story" and cleaned_item["title"].startswith("Ask HN:"):
                categorized_data["ask"].append(cleaned_item)
            elif item_type in categorized_data:
                categorized_data[item_type].append(cleaned_item)
                
        # 4. Upload processed data into separate partitions in Silver Layer
        base_filename = os.path.basename(key) # keeps the original timestamp prefix
        
        for item_type, items_list in categorized_data.items():
            if len(items_list) > 0:
                silver_key = f"{item_type}s/{base_filename}"
                print(f"[INFO] Uploading {len(items_list)} items to s3://{SILVER_BUCKET}/{silver_key}")
                
                s3.put_object(
                    Bucket=SILVER_BUCKET,
                    Key=silver_key,
                    Body=json.dumps(items_list, indent=4)
                )
                
        success_msg = f"[SUCCESS] Data transformation completed. Processed file: {base_filename}"
        print(success_msg)
        return {
            'statusCode': 200,
            'body': json.dumps(success_msg)
        }
        
    except Exception as e:
        error_msg = f"[ERROR] Transformation failed: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(error_msg)
        }