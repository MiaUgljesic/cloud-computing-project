import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
import awswrangler as wr
import boto3
import pandas as pd

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")

HTML_CLEAN_RE = re.compile('<.*?>')


def clean_html(text: str) -> str:
    """Removes HTML tags from raw text fields."""
    if not text:
        return ""
    return re.sub(HTML_CLEAN_RE, '', text)


def parse_hn_timestamp(epoch_time: str | int | None) -> str:
    """Converts a Unix epoch timestamp to an ISO 8601 UTC string."""
    if not epoch_time:
        return datetime.now(timezone.utc).isoformat() + "Z"
    return datetime.fromtimestamp(int(epoch_time), tz=timezone.utc).isoformat().replace("+00:00", "") + "Z"


def parse_twitter_timestamp(iso_str: str | None) -> str:
    """Normalizes Twitter timestamp to ensure it ends with a standard 'Z' suffix."""
    if not iso_str:
        return datetime.now(timezone.utc).isoformat() + "Z"
    return iso_str if iso_str.endswith('Z') else iso_str + "Z"


def _load_json_from_s3(bucket: str, key: str) -> list | dict:
    """Fetches and decodes a JSON payload from an S3 bucket."""
    logger.info(f"Loading raw object from s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


def _process_hn_data(hn_raw_data: list, users_list: list, posts_list: list, post_kids_list: list) -> None:
    """Normalizes raw Hacker News records into relational structures."""
    for item in hn_raw_data:
        username = item.get("by", "unknown_hn_user")
        created_utc = parse_hn_timestamp(item.get("time"))
        post_id = str(item.get("id"))
        
        users_list.append({
            "user_id": str(uuid.uuid4()),
            "username": username,
            "platform": "HackerNews",
            "karma_score": item.get("karma", None),
            "followers_count": None,
            "is_verified": None,
            "created_at": created_utc
        })
        
        posts_list.append({
            "post_id": post_id,
            "author_username": username,
            "content_text": clean_html(item.get("text") or item.get("title") or ""),
            "created_at": created_utc,
            "post_type": item.get("type", "story"),
            "year": created_utc[:4],
            "month": created_utc[5:7],
            "day": created_utc[8:10]
        })

        kids = item.get("kids", [])
        if isinstance(kids, list) and kids:
            for kid_id in kids:
                post_kids_list.append({
                    "parent_post_id": post_id,
                    "kid_comment_id": str(kid_id)
                })


def _process_twitter_data(twitter_raw_data: list, users_list: list, posts_list: list) -> None:
    """Normalizes raw Twitter/X records into relational structures."""
    for tweet in twitter_raw_data:
        username = tweet.get("user", "unknown_x_user")
        created_utc = parse_twitter_timestamp(tweet.get("created_at"))
        
        users_list.append({
            "user_id": str(uuid.uuid4()),
            "username": username,
            "platform": "X",
            "karma_score": None,
            "followers_count": tweet.get("followers_count", 0),
            "is_verified": tweet.get("is_verified", False),
            "created_at": created_utc
        })
        
        posts_list.append({
            "post_id": str(tweet.get("tweet_id")),
            "author_username": username,
            "content_text": clean_html(tweet.get("text", "")),
            "created_at": created_utc,
            "post_type": "tweet",
            "year": created_utc[:4],
            "month": created_utc[5:7],
            "day": created_utc[8:10]
        })


def lambda_handler(event, context):
    logger.info("Starting Silver Layer normalization (3NF Parquet partitioning)...")
    
    try:
        hn_file_key = event.get('HNFileKey')
        twitter_file_key = event.get('TwitterFileKey')
        
        users_list = []
        posts_list = []
        post_kids_list = []

        # 1. Process Hacker News Data
        if hn_file_key:
            hn_raw = _load_json_from_s3(BRONZE_BUCKET, hn_file_key)
            _process_hn_data(hn_raw, users_list, posts_list, post_kids_list)
        
        # 2. Process Twitter Data
        if twitter_file_key:
            twitter_raw = _load_json_from_s3(BRONZE_BUCKET, twitter_file_key)
            _process_twitter_data(twitter_raw, users_list, posts_list)

        # 3. Dataframe Conversion and Deduplication
        logger.info("Converting ingested collections into DataFrames and deduplicating...")
        df_users = pd.DataFrame(users_list).drop_duplicates(subset=['username', 'platform'])
        df_posts = pd.DataFrame(posts_list).drop_duplicates(subset=['post_id'])
        df_kids = pd.DataFrame(post_kids_list).drop_duplicates()

        # 4. Target Paths Mapping
        users_path = f"s3://{SILVER_BUCKET}/users/"
        posts_path = f"s3://{SILVER_BUCKET}/posts/"
        kids_path = f"s3://{SILVER_BUCKET}/post_kids_mapping/"
        
        # 5. Write to Silver Bucket via AWS Wrangler
        logger.info(f"Writing Parquet datasets to Silver Bucket: {SILVER_BUCKET}")
        
        wr.s3.to_parquet(
            df=df_users, path=users_path, dataset=True, 
            partition_cols=["platform"], mode="overwrite_partitions"
        )
        wr.s3.to_parquet(
            df=df_posts, path=posts_path, dataset=True, 
            partition_cols=["year", "month", "day"], mode="overwrite_partitions"
        )
        
        if not df_kids.empty:
            wr.s3.to_parquet(df=df_kids, path=kids_path, dataset=True, mode="overwrite_partitions")
        
        logger.info("Normalization successfully executed in 3NF Parquet format.")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Normalization successfully executed in 3NF Parquet format.',
                'file_key': posts_path
            }
        }
        
    except Exception as e:
        logger.error(f"Silver normalization failed: {str(e)}", exc_info=True)
        raise e