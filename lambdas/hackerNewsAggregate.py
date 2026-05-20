import json
import boto3
import os
import datetime

# Initialize S3 client
s3 = boto3.client('s3')

SILVER_BUCKET = "silver-layer"
GOLD_BUCKET = "gold-layer"

def lambda_handler(event, context):
    print("[INFO] Starting Gold Layer data aggregation and business metrics calculation...")
    
    try:
        # 1. List objects inside the 'storys/' partition of the Silver Layer
        # Note: Using 'storys/' to match our Silver layer folder name precisely
        objects = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix="storys/")
        
        if 'Contents' not in objects:
            print("[WARN] No processed stories found in Silver Layer. Accumulating empty metrics.")
            return {'statusCode': 404, 'body': json.dumps("No data available in Silver Layer.")}
            
        # Get the latest transformed file from Silver
        sorted_objects = sorted(objects['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_key = sorted_objects[0]['Key']
        
        print(f"[INFO] Reading latest silver data from s3://{SILVER_BUCKET}/{latest_key}")
        
        # 2. Read and parse the Silver data
        response = s3.get_object(Bucket=SILVER_BUCKET, Key=latest_key)
        stories = json.loads(response['Body'].read().decode('utf-8'))
        
        total_stories = len(stories)
        total_score = 0
        top_story = {"title": "None", "score": -1, "url": ""}
        
        # 3. Calculate metrics: Total count, Average Score, and Top Story
        for story in stories:
            score = story.get("score", 0)
            total_score += score
            
            if score > top_story["score"]:
                top_story = {
                    "id": story.get("id"),
                    "title": story.get("title"),
                    "score": score,
                    "by": story.get("by"),
                    "url": story.get("url")
                }
                
        average_score = total_score / total_stories if total_stories > 0 else 0
        
        # 4. Construct the Gold Analytical Report
        gold_report = {
            "aggregation_timestamp": datetime.datetime.now().isoformat(),
            "source_file_processed": latest_key,
            "metrics": {
                "total_stories_analyzed": total_stories,
                "average_story_score": round(average_score, 2),
                "highest_rated_story": top_story
            }
        }
        
        # 5. Save the finalized gold report to the Gold Layer bucket
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        gold_key = f"daily_metrics/summary_{timestamp_str}.json"
        
        print(f"[INFO] Uploading gold business report to s3://{GOLD_BUCKET}/{gold_key}")
        s3.put_object(
            Bucket=GOLD_BUCKET,
            Key=gold_key,
            Body=json.dumps(gold_report, indent=4)
        )
        
        success_msg = f"[SUCCESS] Gold analytical report generated successfully at s3://{GOLD_BUCKET}/{gold_key}"
        print(success_msg)
        return {
            'statusCode': 200,
            'body': json.dumps(success_msg)
        }
        
    except Exception as e:
        error_msg = f"[ERROR] Gold aggregation failed: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(error_msg)
        }