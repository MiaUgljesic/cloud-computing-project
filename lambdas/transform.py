import json
import boto3
import os
import re
import uuid
import datetime
import pandas as pd
import awswrangler as wr

s3 = boto3.client('s3')
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET_NAME")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")

def clean_html(text):
    if not text: return ""
    clean_re = re.compile('<.*?>')
    return re.sub(clean_re, '', text)

def parse_hn_timestamp(epoch_time):
    if not epoch_time: return datetime.datetime.utcnow().isoformat() + "Z"
    return datetime.datetime.utcfromtimestamp(int(epoch_time)).isoformat() + "Z"

def parse_twitter_timestamp(iso_str):
    if not iso_str: return datetime.datetime.utcnow().isoformat() + "Z"
    return iso_str if iso_str.endswith('Z') else iso_str + "Z"

def lambda_handler(event, context):
    print("[INFO] Pokretanje oficijalne Silver Layer normalizacije (3NF Parquet)...")
    
    try:
        hn_file_key = event.get('HNFileKey')
        twitter_file_key = event.get('TwitterFileKey')
        
        users_list = []
        posts_list = []
        post_kids_list = [] # NOVO: Lista za Flattening ugnježdenih struktura

        # --- 1. OBRADA HACKER NEWS PODATAKA ---
        hn_response = s3.get_object(Bucket=BRONZE_BUCKET, Key=hn_file_key)
        hn_raw_data = json.loads(hn_response['Body'].read().decode('utf-8'))
        
        for item in hn_raw_data:
            username = item.get("by", "unknown_hn_user")
            created_utc = parse_hn_timestamp(item.get("time"))
            post_id = str(item.get("id"))
            
            users_list.append({
                "user_id": str(uuid.uuid4()),
                "username": username,
                "platform": "HackerNews",
                "karma_score": item.get("karma", None),
                "followers_count": None, # HN nema pratioce
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

            # KLJUČNO (Zahtev 2): Poravnanje (flattening) ugnježdenog 'kids' niza
            kids = item.get("kids", [])
            if isinstance(kids, list) and len(kids) > 0:
                for kid_id in kids:
                    post_kids_list.append({
                        "parent_post_id": post_id,
                        "kid_comment_id": str(kid_id)
                    })

        # --- 2. OBRADA TWITTER PODATAKA ---
        twitter_response = s3.get_object(Bucket=BRONZE_BUCKET, Key=twitter_file_key)
        twitter_raw_data = json.loads(twitter_response['Body'].read().decode('utf-8'))
        
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

        # --- 3. PRETVARANJE U DATAFRAME I UKLANJANJE DUPLIKATA ---
        df_users = pd.DataFrame(users_list).drop_duplicates(subset=['username', 'platform'])
        df_posts = pd.DataFrame(posts_list).drop_duplicates(subset=['post_id'])
        df_kids = pd.DataFrame(post_kids_list).drop_duplicates()

        # --- 4. UPIS U S3 PREKO AWS WRANGLER-A (PARQUET) ---
        users_path = f"s3://{SILVER_BUCKET}/users/"
        posts_path = f"s3://{SILVER_BUCKET}/posts/"
        kids_path = f"s3://{SILVER_BUCKET}/post_kids_mapping/"
        
        wr.s3.to_parquet(df=df_users, path=users_path, dataset=True, partition_cols=["platform"], mode="overwrite_partitions")
        wr.s3.to_parquet(df=df_posts, path=posts_path, dataset=True, partition_cols=["year", "month", "day"], mode="overwrite_partitions")
        
        # Upisujemo izravnane veze između postova i komentara
        if not df_kids.empty:
            wr.s3.to_parquet(df=df_kids, path=kids_path, dataset=True, mode="overwrite_partitions")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Normalizacija uspešno izvršena u 3NF Parquet formatu.',
                'file_key': posts_path # Prosleđujemo Gold layer-u referencu
            }
        }
        
    except Exception as e:
        print(f"[ERROR] Silver normalization failed: {str(e)}")
        raise e