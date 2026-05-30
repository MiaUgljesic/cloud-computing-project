import json
import boto3
import os
import datetime
import random

s3 = boto3.client('s3')

# Extract the bucket name from the configured environment variables
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

def lambda_handler(event, context):
    print("[INFO] Starting X (Twitter) data ingestion pipeline...")
    
    try:
        # Production dataset simulation (generating mockup tweets based on real datasets)
        # In a real scenario, you could load a local CSV/JSON file containing Bitcoin/Tech tweets here
        topics = ["Bitcoin", "AI", "CloudComputing", "Serverless", "Python"]
        mock_tweets = []
        
        # Generate 15 tweets for a better sample size
        for i in range(15):
            mock_tweets.append({
                "tweet_id": str(random.randint(1000000000, 9999999999)),
                "created_at": (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat() + "Z",
                "user": f"tech_user_{random.randint(1, 100)}",
                "text": f"Great day to learn about {random.choice(topics)} and AWS Lambda functions! #cloud",
                "retweet_count": random.randint(0, 50),
                "favorite_count": random.randint(0, 150),
                "followers_count": random.randint(10, 50000),  
                "is_verified": random.choice([True, False]),   
                "lang": "en"
            })
            
        # Create a unique key (partitioning by source: twitter/)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"twitter/raw_tweets_{timestamp}.json"
        
        # Write to the Bronze layer in its raw format
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(mock_tweets, indent=4)
        )
        
        print(f"[INFO] Successfully saved raw Twitter data to {BUCKET_NAME}/{s3_key}")
        
        # Return a structured response for the Step Function
        return {
            'statusCode': 200,
            'body': {
                'message': 'X Ingestion successful',
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        error_msg = f"[ERROR] X Ingestion failed: {str(e)}"
        print(error_msg)
        raise e