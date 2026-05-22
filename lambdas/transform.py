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
    """Uklanja HTML tagove iz teksta (zahtev 2 - Čišćenje vrednosti)"""
    if not text:
        return ""
    clean_re = re.compile('<.*?>')
    return re.sub(clean_re, '', text)

def parse_hn_timestamp(epoch_time):
    """Pretvara Unix Epoch u ISO-8601 UTC string (zahtev 2 - Poravnanje vremena)"""
    if not epoch_time:
        return datetime.datetime.utcnow().isoformat() + "Z"
    return datetime.datetime.utcfromtimestamp(int(epoch_time)).isoformat() + "Z"

def parse_twitter_timestamp(iso_str):
    """Osigurava da je Twitter vreme u ispravnom formatu"""
    if not iso_str:
        return datetime.datetime.utcnow().isoformat() + "Z"
    # Ako već ima 'Z' ili +00:00, standardizujemo ga
    return iso_str if iso_str.endswith('Z') else iso_str + "Z"

def lambda_handler(event, context):
    print("[INFO] Pokretanje oficijalne Silver Layer normalizacije (3NF Parquet)...")
    
    try:
        hn_file_key = event.get('HNFileKey')
        twitter_file_key = event.get('TwitterFileKey')
        
        if not hn_file_key or not twitter_file_key:
            raise KeyError("Nedostaju HNFileKey ili TwitterFileKey u payload-u.")
            
        users_list = []
        posts_list = []

        # --- 1. OBRADA HACKER NEWS PODATAKA ---
        print(f"[INFO] Čitanje HN fajla: {hn_file_key}")
        hn_response = s3.get_object(Bucket=BRONZE_BUCKET, Key=hn_file_key)
        hn_raw_data = json.loads(hn_response['Body'].read().decode('utf-8'))
        
        for item in hn_raw_data:
            username = item.get("by", "unknown_hn_user")
            created_utc = parse_hn_timestamp(item.get("time"))
            
            # Kreiranje User zapisa (zahtev za users tabelu)
            users_list.append({
                "user_id": str(uuid.uuid4()),
                "username": username,
                "platform": "HackerNews",
                "karma_score": item.get("karma", None),  # HN ima karmu
                "is_verified": None,                     # HN nema verifikaciju
                "created_at": created_utc
            })
            
            # Kreiranje Post zapisa (zahtev za posts tabelu)
            posts_list.append({
                "post_id": str(item.get("id")),
                "author_username": username,
                "content_text": clean_html(item.get("text") or item.get("title") or ""),
                "created_at": created_utc,
                "post_type": item.get("type", "story"),
                # Dodajemo kolone za particionisanje posta (biće uklonjene iz samog fajla ali kreiraju foldere)
                "year": created_utc[:4],
                "month": created_utc[5:7],
                "day": created_utc[8:10]
            })

        # --- 2. OBRADA TWITTER PODATAKA ---
        print(f"[INFO] Čitanje Twitter fajla: {twitter_file_key}")
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
                "is_verified": tweet.get("is_verified", False), # X specifično
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

        # --- 4. UPIS U S3 PREKO AWS WRANGLER-A (PARQUET + PARTICIONISANJE) ---
        users_path = f"s3://{SILVER_BUCKET}/users/"
        posts_path = f"s3://{SILVER_BUCKET}/posts/"
        
        print(f"[INFO] Upisivanje USERS tabele u Parquet na: {users_path}")
        wr.s3.to_parquet(
            df=df_users,
            path=users_path,
            dataset=True,
            partition_cols=["platform"],  # Particionisanje po platformi
            mode="overwrite_partitions"
        )
        
        print(f"[INFO] Upisivanje POSTS tabele u Parquet na: {posts_path}")
        wr.s3.to_parquet(
            df=df_posts,
            path=posts_path,
            dataset=True,
            partition_cols=["year", "month", "day"],  # Particionisanje po datumu
            mode="overwrite_partitions"
        )
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Normalizacija uspešno izvršena u 3NF Parquet formatu.',
                'users_path': users_path,
                'posts_path': posts_path,
                # Prosleđujemo statičke rute ili metapodatke za sledeći (Gold) korak ako su mu potrebni
                'execution_date': datetime.datetime.now().strftime("%Y-%m-%d")
            }
        }
        
    except Exception as e:
        error_msg = f"[ERROR] Silver normalization failed: {str(e)}"
        print(error_msg)
        raise e