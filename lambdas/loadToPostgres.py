import json
import logging
import os
import pandas as pd
import awswrangler as wr
import boto3
import pg8000

# Setup structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

GOLD_BUCKET = os.environ.get("GOLD_BUCKET_NAME")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "gold_analytics")
DB_USER = os.environ.get("DB_USER", "analytics_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

# Table DDL - created on first run if they don't already exist.
# Each table carries a `report_date` column so re-runs for the same day are idempotent
# (we delete existing rows for that date before inserting, see _replace_day()).
TABLE_DDL = {
    "daily_users_metric": """
        CREATE TABLE IF NOT EXISTS daily_users_metric (
            report_date DATE NOT NULL,
            platform VARCHAR(32) NOT NULL,
            total_users INTEGER,
            new_users INTEGER,
            data_quality_score NUMERIC(5,2),
            PRIMARY KEY (report_date, platform)
        );
    """,
    "hn_daily_counts": """
        CREATE TABLE IF NOT EXISTS hn_daily_counts (
            report_date DATE PRIMARY KEY,
            stories_count INTEGER,
            asks_count INTEGER,
            comments_count INTEGER,
            jobs_count INTEGER,
            polls_count INTEGER
        );
    """,
    "data_quality_kpi": """
        CREATE TABLE IF NOT EXISTS data_quality_kpi (
            report_date DATE PRIMARY KEY,
            data_quality_kpi_percent NUMERIC(5,2)
        );
    """,
    "top_x_users_by_followers": """
        CREATE TABLE IF NOT EXISTS top_x_users_by_followers (
            report_date DATE NOT NULL,
            rank SMALLINT NOT NULL,
            username VARCHAR(255),
            followers_count INTEGER,
            PRIMARY KEY (report_date, rank)
        );
    """,
    "top_hn_users_highest_karma": """
        CREATE TABLE IF NOT EXISTS top_hn_users_highest_karma (
            report_date DATE NOT NULL,
            rank SMALLINT NOT NULL,
            username VARCHAR(255),
            karma_score INTEGER,
            PRIMARY KEY (report_date, rank)
        );
    """,
    "top_hn_users_lowest_karma": """
        CREATE TABLE IF NOT EXISTS top_hn_users_lowest_karma (
            report_date DATE NOT NULL,
            rank SMALLINT NOT NULL,
            username VARCHAR(255),
            karma_score INTEGER,
            PRIMARY KEY (report_date, rank)
        );
    """,
    "top_hn_jobs_highest_score": """
        CREATE TABLE IF NOT EXISTS top_hn_jobs_highest_score (
            report_date DATE NOT NULL,
            rank SMALLINT NOT NULL,
            post_id VARCHAR(64),
            author_username VARCHAR(255),
            score INTEGER,
            PRIMARY KEY (report_date, rank)
        );
    """,
    "top_hn_stories_highest_score": """
        CREATE TABLE IF NOT EXISTS top_hn_stories_highest_score (
            report_date DATE NOT NULL,
            rank SMALLINT NOT NULL,
            post_id VARCHAR(64),
            content_text TEXT,
            score INTEGER,
            PRIMARY KEY (report_date, rank)
        );
    """,
}


def _ensure_tables_exist(con) -> None:
    """Creates every target table if it doesn't already exist."""
    with con.cursor() as cur:
        for table_name, ddl in TABLE_DDL.items():
            logger.info(f"Ensuring table exists: {table_name}")
            cur.execute(ddl)
    con.commit()


def _load_gold_report(report_key: str) -> dict:
    """Fetches the consolidated analytical snapshot JSON produced by AggregateFunction."""
    logger.info(f"Reading gold report from s3://{GOLD_BUCKET}/{report_key}")
    response = s3_client.get_object(Bucket=GOLD_BUCKET, Key=report_key)
    return json.loads(response['Body'].read().decode('utf-8'))


def _replace_day(con, table_name: str, report_date: str, df: pd.DataFrame) -> None:
    """Deletes any existing rows for report_date, then inserts df. Makes reruns idempotent."""
    if df.empty:
        logger.info(f"No rows to load for {table_name} on {report_date}, skipping.")
        return

    with con.cursor() as cur:
        cur.execute(f"DELETE FROM {table_name} WHERE report_date = %s", (report_date,))
    con.commit()

    for col in df.columns:
    # Ako je kolona tipa float, pokušaj konverziju u int (nakon popunjavanja NaN sa 0)
        if df[col].dtype == 'float64':
            df[col] = df[col].fillna(0).astype(int)

    wr.postgresql.to_sql(
        df=df,
        con=con,
        table=table_name,
        schema="public",
        mode="append",
        use_column_names=True,
        index=False,
    )
    logger.info(f"Loaded {len(df)} rows into {table_name} for {report_date}")


def _leaderboard_to_df(items: list, report_date: str) -> pd.DataFrame:
    """Adds report_date + rank columns to a leaderboard list from the gold report."""
    if not items:
        return pd.DataFrame()
    df = pd.DataFrame(items)
    df.insert(0, "report_date", report_date)
    df.insert(1, "rank", range(1, len(df) + 1))
    return df


def lambda_handler(event, context):
    logger.info("Starting Gold -> PostgreSQL load for Superset...")

    report_key = event.get("ReportKey")
    if not report_key:
        raise ValueError("Missing required 'ReportKey' input from the AggregateToGold step")

    con = None
    try:
        report = _load_gold_report(report_key)
        report_date = report["date"]

        con = pg8000.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )

        _ensure_tables_exist(con)

        # 1. Data quality KPI
        _replace_day(
            con, "data_quality_kpi", report_date,
            pd.DataFrame([{"report_date": report_date, "data_quality_kpi_percent": report["data_quality_kpi_percent"]}])
        )

        # 2. Hacker News daily counts
        hn_counts = report["hacker_news_daily_counts"]
        _replace_day(
            con, "hn_daily_counts", report_date,
            pd.DataFrame([{
                "report_date": report_date,
                "stories_count": hn_counts["stories_count"],
                "asks_count": hn_counts["asks_count"],
                "comments_count": hn_counts["comments_count"],
                "jobs_count": hn_counts["jobs_count"],
                "polls_count": hn_counts["polls_count"],
            }])
        )

        # 3. Leaderboards
        leaderboard_tables = {
            "top_x_users_by_followers": "top_10_x_users_by_followers",
            "top_hn_users_highest_karma": "top_10_hn_users_highest_karma",
            "top_hn_users_lowest_karma": "top_10_hn_users_lowest_karma",
            "top_hn_jobs_highest_score": "top_10_hn_jobs_highest_score",
            "top_hn_stories_highest_score": "top_10_hn_stories_highest_score",
        }
        for table_name, report_field in leaderboard_tables.items():
            df = _leaderboard_to_df(report.get(report_field, []), report_date)
            _replace_day(con, table_name, report_date, df)

        # Note: daily_users_metric is written separately by AggregateFunction directly to
        # S3 parquet (partitioned by platform/date). We mirror the same-day slice into
        # Postgres here so Superset only needs one data source.
        daily_users_path = f"s3://{GOLD_BUCKET}/daily_users_metric/"
        try:
            df_daily_users = wr.s3.read_parquet(path=daily_users_path, dataset=True)
            df_daily_users = df_daily_users[df_daily_users["date"] == report_date].rename(
                columns={"date": "report_date"}
            )[["report_date", "platform", "total_users", "new_users", "data_quality_score"]]
            _replace_day(con, "daily_users_metric", report_date, df_daily_users)
        except Exception as parquet_err:
            logger.warning(f"Could not load daily_users_metric parquet: {parquet_err}")

        logger.info("Gold -> PostgreSQL load completed successfully.")

        return {
            "statusCode": 200,
            "body": {
                "message": "Gold layer metrics successfully loaded into PostgreSQL for Superset.",
                "report_date": report_date,
            }
        }

    except Exception as e:
        logger.error(f"Load-to-Postgres failed: {str(e)}", exc_info=True)
        raise e
    finally:
        if con is not None:
            con.close()