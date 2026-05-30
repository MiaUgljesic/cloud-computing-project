import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
import boto3

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")
TOPICS = ["Bitcoin", "AI", "CloudComputing", "Serverless", "Python"]


def _generate_mock_tweets(count: int = 15) -> list:
    """Generates a list of mock tweets simulating real tech dataset samples."""
    now_utc = datetime.now(timezone.utc)
    past_utc = now_utc - timedelta(days=1)
    
    return [
        {
            "tweet_id": str(random.randint(1000000000, 9999999999)),
            "created_at": f"{past_utc.isoformat(timespec='seconds').replace('+00:00', '')}Z",
            "user": f"tech_user_{random.randint(1, 100)}",
            "text": f"Great day to learn about {random.choice(TOPICS)} and AWS Lambda functions! #cloud",
            "retweet_count": random.randint(0, 50),
            "favorite_count": random.randint(0, 150),
            "followers_count": random.randint(10, 50000),  
            "is_verified": random.choice([True, False]),   
            "lang": "en"
        }
        for _ in range(count)
    ]


def lambda_handler(event, context):
    logger.info("Starting X (Twitter) data ingestion pipeline...")
    
    try:
        mock_tweets = _generate_mock_tweets(count=15)
        
        # Create a unique key partitioned by source folder
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"twitter/raw_tweets_{timestamp}.json"
        
        logger.info(f"Uploading raw data to S3 bucket: {BUCKET_NAME} with key: {s3_key}")
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(mock_tweets, indent=4)
        )
        
        logger.info("Successfully saved raw Twitter data to S3.")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'X Ingestion successful',
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        logger.error(f"X Ingestion failed: {str(e)}", exc_info=True)
        raise e