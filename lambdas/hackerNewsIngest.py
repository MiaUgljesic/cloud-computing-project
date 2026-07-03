import json
import logging
import os
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

HN_ITEM_BASE_URL = "https://hacker-news.firebaseio.com/v0"
ALGOLIA_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"

ALGOLIA_TAGS = ["story", "ask_hn", "job", "poll", "show_hn"]

HITS_PER_PAGE = 1000
MAX_PAGES_PER_TAG = 5 
MAX_COMMENTS_TOTAL = 500


def _http_get_json(url: str) -> dict | list | None:
    """Performs an HTTP GET and parses the JSON body, returning None on any failure."""
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        logger.warning(f"HTTP GET failed for URL {url}: {str(e)}")
        return None


def _fetch_item(item_id: int | str) -> dict | None:
    """Fetches a single item (story, comment, etc.) from the HN Firebase API.

    We still use the Firebase item endpoint (not Algolia's hit shape) so the
    raw bronze payload matches HN's native item schema exactly, per the
    "store data in its original form" bronze-layer requirement.
    """
    url = f"{HN_ITEM_BASE_URL}/item/{item_id}.json"
    return _fetch_item_raw(url)


def _fetch_item_raw(url: str) -> dict | None:
    data = _http_get_json(url)
    return data if isinstance(data, dict) else None


def _fetch_user(username: str) -> dict | None:
    """Fetches a HN user profile (contains real 'karma') from the HN API."""
    if not username:
        return None
    url = f"{HN_ITEM_BASE_URL}/user/{username}.json"
    data = _http_get_json(url)
    return data if isinstance(data, dict) else None


def _fetch_algolia_ids_for_tag(tag: str, start_epoch: int, end_epoch: int) -> list[int]:
    """
    Returns everything
    from the target day rather than whatever happened to still be in a
    live cache at call time.
    """
    ids = []
    numeric_filter = f"created_at_i>={start_epoch},created_at_i<{end_epoch}"

    for page in range(MAX_PAGES_PER_TAG):
        query = urllib.parse.urlencode({
            "tags": tag,
            "numericFilters": numeric_filter,
            "hitsPerPage": HITS_PER_PAGE,
            "page": page,
        })
        url = f"{ALGOLIA_SEARCH_URL}?{query}"
        result = _http_get_json(url)

        if not result or "hits" not in result:
            logger.warning(f"No/invalid Algolia response for tag={tag} page={page}")
            break

        hits = result["hits"]
        if not hits:
            break

        for hit in hits:
            object_id = hit.get("objectID")
            if object_id is not None:
                ids.append(int(object_id))

        total_pages = result.get("nbPages", 1)
        if page + 1 >= total_pages:
            break

    logger.info(f"Algolia returned {len(ids)} ids for tag={tag} in target window.")
    return ids


def lambda_handler(event, context):
    logger.info("Starting Hacker News data ingestion pipeline (Algolia-backed)...")

    try:
        now_utc = datetime.now(timezone.utc)
        today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)
        day_start_epoch = int(yesterday_start.timestamp())
        day_end_epoch = int(today_start.timestamp())

        logger.info(
            f"Collecting HN items created between {yesterday_start.isoformat()} "
            f"and {today_start.isoformat()} (UTC)"
        )

        all_item_ids = set()

        for tag in ALGOLIA_TAGS:
            logger.info(f"Querying Algolia for tag: {tag}")
            tag_ids = _fetch_algolia_ids_for_tag(tag, day_start_epoch, day_end_epoch)
            all_item_ids.update(tag_ids)

        logger.info(f"Total distinct top-level items to fetch: {len(all_item_ids)}")

        fetched_items = []
        comment_ids = []
        usernames_seen = set()

        for item_id in all_item_ids:
            item = _fetch_item(item_id)
            if not item:
                continue
            fetched_items.append(item)

            author = item.get("by")
            if author:
                usernames_seen.add(author)

            if isinstance(item.get("kids"), list):
                comment_ids.extend(item["kids"])

        logger.info(f"Collected {len(comment_ids)} candidate comment ids. Fetching up to {MAX_COMMENTS_TOTAL}...")
        fetched_comments = 0
        for cid in comment_ids:
            if fetched_comments >= MAX_COMMENTS_TOTAL:
                logger.info("Hit comment fetch cap, stopping.")
                break
            comment = _fetch_item(cid)
            if not comment:
                continue
            comment_time = comment.get("time")
            if comment_time is not None and day_start_epoch <= int(comment_time) < day_end_epoch:
                fetched_items.append(comment)
                fetched_comments += 1
                author = comment.get("by")
                if author:
                    usernames_seen.add(author)

        logger.info(f"Fetching {len(usernames_seen)} user profiles for karma data...")
        user_profiles = {}
        for username in usernames_seen:
            profile = _fetch_user(username)
            if profile:
                user_profiles[username] = profile

        payload = {
            "items": fetched_items,
            "users": list(user_profiles.values()),
        }

        timestamp = now_utc.strftime("%Y%m%d_%H%M%S")
        s3_key = f"hacker_news/raw_stories_{timestamp}.json"

        logger.info(
            f"Uploading {len(fetched_items)} items and {len(user_profiles)} user profiles "
            f"to S3 bucket: {BUCKET_NAME}"
        )

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(payload, indent=4)
        )

        success_msg = (
            f"Successfully ingested {len(fetched_items)} HN items and "
            f"{len(user_profiles)} user profiles for {yesterday_start.date()}."
        )
        logger.info(success_msg)

        return {
            'statusCode': 200,
            'body': {
                'message': success_msg,
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        raise e