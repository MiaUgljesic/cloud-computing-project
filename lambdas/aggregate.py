import json
import logging
import os
from datetime import datetime, timezone, timedelta
import awswrangler as wr
import boto3
import pandas as pd

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

SILVER_BUCKET = os.environ.get("SILVER_BUCKET_NAME")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME")


def calculate_data_quality_score(df: pd.DataFrame) -> float:
    """
    KPI: Data Quality Score
    Shows the percentage of cells in the dataframe that are NOT null.
    Formula: (number_of_non_null_values / total_number_of_cells) * 100
    """
    if df.empty:
        return 100.0
    total_cells = df.size
    non_null_cells = df.notnull().sum().sum()
    return round((non_null_cells / total_cells) * 100, 2)


def _build_hn_metrics(df_posts: pd.DataFrame, current_date: str) -> dict:
    """Calculates daily post counts by type on Hacker News."""
    df_hn_posts = df_posts[df_posts['post_type'] != 'tweet']
    hn_counts = df_hn_posts['post_type'].value_counts()

    return {
        "date": current_date,
        "stories_count": int(hn_counts.get('story', 0)),
        "asks_count": int(hn_counts.get('ask', 0)),
        "comments_count": int(hn_counts.get('comment', 0)),
        "jobs_count": int(hn_counts.get('job', 0)),
        "polls_count": int(hn_counts.get('poll', 0))
    }


def _get_previous_day_totals(current_date: str) -> dict:
    """
    Reads yesterday's daily_users_metric partition from Gold (if it exists) to
    compute a real new_users figure via day-over-day diff. Returns {} if no
    prior partition exists (e.g. first run).
    """
    try:
        prev_date = (
            datetime.strptime(current_date, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        path = f"s3://{GOLD_BUCKET}/daily_users_metric/"
        df_prev = wr.s3.read_parquet(path=path, dataset=True)
        df_prev = df_prev[df_prev["date"] == prev_date]
        if df_prev.empty:
            return {}
        return dict(zip(df_prev["platform"], df_prev["total_users"]))
    except Exception as e:
        logger.warning(f"Could not read previous day's daily_users_metric (may not exist yet): {e}")
        return {}


def _generate_daily_users_metrics(df_users: pd.DataFrame, current_date: str, avg_dq_score: float) -> pd.DataFrame:
    """
    Generates daily user metrics grouped by platform.

    total_users = distinct users seen for that platform in this run's silver
    snapshot. new_users = total_users - previous day's total_users for that
    platform (floored at 0; falls back to total_users if no prior day exists).
    """
    users_metrics_list = []
    platforms_mapping = {
        'Hacker News': 'HackerNews',
        'X': 'X'
    }

    previous_totals = _get_previous_day_totals(current_date)

    for display_name, silver_name in platforms_mapping.items():
        df_plat_users = df_users[df_users['platform'] == silver_name]
        total_users_count = len(df_plat_users)
        platform_key = display_name.replace(' ', '')

        prev_total = previous_totals.get(platform_key)
        if prev_total is None:
            new_users_count = total_users_count
        else:
            new_users_count = max(0, total_users_count - int(prev_total))

        users_metrics_list.append({
            "date": current_date,
            "platform": platform_key,
            "total_users": total_users_count,
            "new_users": new_users_count,
            "data_quality_score": avg_dq_score
        })

    df = pd.DataFrame(users_metrics_list)
    for col in ["total_users", "new_users"]:
        df[col] = df[col].astype("Int64")
    return df


def _generate_leaderboards(df_users: pd.DataFrame, df_posts: pd.DataFrame) -> dict:
    """
    Generates Top 10 leaderboards using only real, observed fields. If a
    metric is missing for every row (e.g. no karma data collected at all),
    the leaderboard is returned empty.
    """
    # 1. Top 10 X users by followers
    df_x_users = df_users[df_users['platform'] == 'X'].copy()
    if 'followers_count' in df_x_users.columns:
        df_x_users = df_x_users.dropna(subset=['followers_count'])
    else:
        df_x_users = df_x_users.iloc[0:0]

    if not df_x_users.empty:
        df_x_users['followers_count'] = df_x_users['followers_count'].astype("Int64")
        top_10_x = df_x_users.nlargest(10, 'followers_count')[['username', 'followers_count']]
    else:
        top_10_x = pd.DataFrame(columns=['username', 'followers_count'])

    # 2. Top 10 Hacker News users by karma 
    df_hn_users = df_users[df_users['platform'] == 'HackerNews'].copy()
    if 'karma_score' in df_hn_users.columns:
        df_hn_users_with_karma = df_hn_users.dropna(subset=['karma_score'])
    else:
        df_hn_users_with_karma = df_hn_users.iloc[0:0]

    if not df_hn_users_with_karma.empty:
        df_hn_users_with_karma['karma_score'] = df_hn_users_with_karma['karma_score'].astype("Int64")
        top_10_hn_high = df_hn_users_with_karma.nlargest(10, 'karma_score')[['username', 'karma_score']]
        top_10_hn_low = df_hn_users_with_karma.nsmallest(10, 'karma_score')[['username', 'karma_score']]
    else:
        top_10_hn_high = pd.DataFrame(columns=['username', 'karma_score'])
        top_10_hn_low = pd.DataFrame(columns=['username', 'karma_score'])

    # 3. Top 10 Hacker News posts/jobs by real HN score points field
    df_posts_eval = df_posts.copy()
    if 'score' in df_posts_eval.columns:
        df_posts_eval = df_posts_eval.dropna(subset=['score'])
    else:
        df_posts_eval = df_posts_eval.iloc[0:0]

    if not df_posts_eval.empty:
        df_posts_eval['score'] = df_posts_eval['score'].astype("Int64")
        top_10_jobs = df_posts_eval[df_posts_eval['post_type'] == 'job'].nlargest(10, 'score')[['post_id', 'author_username', 'score']]
        top_10_stories = df_posts_eval[df_posts_eval['post_type'] == 'story'].nlargest(10, 'score')[['post_id', 'content_text', 'score']]
    else:
        top_10_jobs = pd.DataFrame(columns=['post_id', 'author_username', 'score'])
        top_10_stories = pd.DataFrame(columns=['post_id', 'content_text', 'score'])

    return {
        "top_10_x_users_by_followers": top_10_x.to_dict(orient='records'),
        "top_10_hn_users_highest_karma": top_10_hn_high.to_dict(orient='records'),
        "top_10_hn_users_lowest_karma": top_10_hn_low.to_dict(orient='records'),
        "top_10_hn_jobs_highest_score": top_10_jobs.to_dict(orient='records'),
        "top_10_hn_stories_highest_score": top_10_stories.to_dict(orient='records')
    }


def lambda_handler(event, context):
    logger.info("Starting Gold Layer analytics generation (Star Schema & KPIs)...")

    try:
        silver_users_path = f"s3://{SILVER_BUCKET}/users/"
        silver_posts_path = f"s3://{SILVER_BUCKET}/posts/"

        logger.info(f"Loading data layers via AWS Wrangler from Silver Bucket: {SILVER_BUCKET}")
        df_users = wr.s3.read_parquet(path=silver_users_path, dataset=True)
        df_posts = wr.s3.read_parquet(path=silver_posts_path, dataset=True)

        current_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Calculate Quality Metrics KPI
        dq_users = calculate_data_quality_score(df_users)
        dq_posts = calculate_data_quality_score(df_posts)
        avg_dq_score = round((dq_users + dq_posts) / 2, 2)
        logger.info(f"Global Lake Data Quality Score calculated: {avg_dq_score}%")

        
        df_daily_users = _generate_daily_users_metrics(df_users, current_date_str, avg_dq_score)
        hn_metrics = _build_hn_metrics(df_posts, current_date_str)
        leaderboards = _generate_leaderboards(df_users, df_posts)

        
        gold_users_metric_path = f"s3://{GOLD_BUCKET}/daily_users_metric/"
        logger.info(f"Writing daily users fact/metrics to: {gold_users_metric_path}")

        wr.s3.to_parquet(
            df=df_daily_users,
            path=gold_users_metric_path,
            dataset=True,
            partition_cols=["platform", "date"],
            mode="overwrite_partitions",
            dtype={"total_users": "bigint", "new_users": "bigint", "data_quality_score": "double"},
        )

        
        summary_report = {
            "date": current_date_str,
            "data_quality_kpi_percent": avg_dq_score,
            "hacker_news_daily_counts": hn_metrics,
            **leaderboards
        }

        report_key = f"analytical_snapshots/date={current_date_str}/report.json"
        logger.info(f"Saving final analytical snapshot report to: s3://{GOLD_BUCKET}/{report_key}")

        s3_client.put_object(
            Bucket=GOLD_BUCKET,
            Key=report_key,
            Body=json.dumps(summary_report, indent=4, ensure_ascii=False)
        )

        logger.info("Gold Layer analytics processing successfully completed.")

        return {
            'statusCode': 200,
            'body': {
                'message': 'Gold Layer successfully generated with all KPIs and metrics structured by Star Schema.',
                'gold_parquet_path': gold_users_metric_path,
                'gold_json_report': report_key,
                'data_quality_score': f"{avg_dq_score}%"
            }
        }

    except Exception as e:
        logger.error(f"Gold layer processing failed: {str(e)}", exc_info=True)
        raise e
