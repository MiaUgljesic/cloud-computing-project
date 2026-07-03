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


def _process_hn_data(hn_raw: dict, users_list: list, posts_list: list, post_kids_list: list) -> None:
    """Normalizes raw Hacker News records into relational structures.

    Expects hn_raw = {"items": [...], "users": [...]} as produced by the
    Algolia-backed hackerNewsIngest lambda. "users" contains real HN user
    profiles (with true karma), fetched from /v0/user/{username}.json since
    the item endpoint never carries karma.
    """
    hn_items = hn_raw.get("items", []) if isinstance(hn_raw, dict) else hn_raw
    hn_user_profiles = hn_raw.get("users", []) if isinstance(hn_raw, dict) else []

    karma_by_username = {
        profile.get("id"): profile.get("karma")
        for profile in hn_user_profiles
        if profile.get("id")
    }
    created_by_username = {
        profile.get("id"): profile.get("created")
        for profile in hn_user_profiles
        if profile.get("id")
    }

    for username, karma in karma_by_username.items():
        created_epoch = created_by_username.get(username)
        users_list.append({
            "user_id": str(uuid.uuid4()),
            "username": username,
            "platform": "HackerNews",
            "karma_score": karma,
            "followers_count": None,
            "is_verified": None,
            "created_at": parse_hn_timestamp(created_epoch),
        })

    for item in hn_items:
        username = item.get("by", "unknown_hn_user")
        created_utc = parse_hn_timestamp(item.get("time"))
        post_id = str(item.get("id"))
        post_type = item.get("type", "story")
        score = item.get("score")

        if username not in karma_by_username:
            users_list.append({
                "user_id": str(uuid.uuid4()),
                "username": username,
                "platform": "HackerNews",
                "karma_score": None,
                "followers_count": None,
                "is_verified": None,
                "created_at": created_utc,
            })
            karma_by_username[username] = None

        posts_list.append({
            "post_id": post_id,
            "author_username": username,
            "content_text": clean_html(item.get("text") or item.get("title") or ""),
            "created_at": created_utc,
            "post_type": post_type,
            "score": score,
            "year": created_utc[:4],
            "month": created_utc[5:7],
            "day": created_utc[8:10],
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
            "followers_count": tweet.get("followers_count"),
            "is_verified": tweet.get("is_verified", False),
            "created_at": created_utc
        })

        posts_list.append({
            "post_id": str(tweet.get("tweet_id")),
            "author_username": username,
            "content_text": clean_html(tweet.get("text", "")),
            "created_at": created_utc,
            "post_type": "tweet",
            "score": tweet.get("favorite_count"),
            "year": created_utc[:4],
            "month": created_utc[5:7],
            "day": created_utc[8:10]
        })


def _finalize_users_dtypes(df_users: pd.DataFrame) -> pd.DataFrame:
    """
    Locks nullable numeric columns to pandas' nullable Int64 dtype so every
    run of this lambda produces the exact same physical schema, regardless of
    whether nulls happen to be present. Without this, pandas infers float64
    when NaNs exist and int64 when they don't, which causes Athena/AWS
    Wrangler to fail with "incompatible types: double vs int64" when merging
    partitions written by different invocations.
    """
    if df_users.empty:
        return df_users
    for col in ["karma_score", "followers_count"]:
        if col in df_users.columns:
            df_users[col] = df_users[col].astype("Int64")
    if "is_verified" in df_users.columns:
        df_users["is_verified"] = df_users["is_verified"].astype("boolean")
    return df_users


def lambda_handler(event, context):
    logger.info("Starting Silver Layer normalization (3NF Parquet partitioning)...")

    try:
        hn_file_key = event.get('HNFileKey')
        twitter_file_key = event.get('TwitterFileKey')

        users_list = []
        posts_list = []
        post_kids_list = []

        if hn_file_key:
            hn_raw = _load_json_from_s3(BRONZE_BUCKET, hn_file_key)
            _process_hn_data(hn_raw, users_list, posts_list, post_kids_list)

        if twitter_file_key:
            twitter_raw = _load_json_from_s3(BRONZE_BUCKET, twitter_file_key)
            _process_twitter_data(twitter_raw, users_list, posts_list)

        logger.info("Converting ingested collections into DataFrames and deduplicating...")
        df_users_raw = pd.DataFrame(users_list)
        if not df_users_raw.empty:
            has_karma = df_users_raw["karma_score"].notna() if "karma_score" in df_users_raw else False
            has_followers = df_users_raw["followers_count"].notna() if "followers_count" in df_users_raw else False
            df_users_raw["_has_signal"] = has_karma | has_followers
            df_users = (
                df_users_raw.sort_values("_has_signal", ascending=False)
                .drop_duplicates(subset=['username', 'platform'], keep='first')
                .drop(columns=["_has_signal"])
            )
        else:
            df_users = df_users_raw

        df_users = _finalize_users_dtypes(df_users)

        df_posts = pd.DataFrame(posts_list).drop_duplicates(subset=['post_id'])
        if not df_posts.empty and "score" in df_posts.columns:
            df_posts["score"] = df_posts["score"].astype("Int64")

        df_kids = pd.DataFrame(post_kids_list).drop_duplicates()

        users_path = f"s3://{SILVER_BUCKET}/users/"
        posts_path = f"s3://{SILVER_BUCKET}/posts/"
        kids_path = f"s3://{SILVER_BUCKET}/post_kids_mapping/"

        logger.info(f"Writing Parquet datasets to Silver Bucket: {SILVER_BUCKET}")

        wr.s3.to_parquet(
            df=df_users,
            path=users_path,
            dataset=True,
            partition_cols=["platform"],
            mode="overwrite_partitions",
            dtype={"karma_score": "bigint", "followers_count": "bigint"},
        )
        wr.s3.to_parquet(
            df=df_posts,
            path=posts_path,
            dataset=True,
            partition_cols=["year", "month", "day"],
            mode="overwrite_partitions",
            dtype={"score": "bigint"},
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
