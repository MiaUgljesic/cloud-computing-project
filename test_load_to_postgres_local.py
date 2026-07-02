"""
Brzi lokalni test za loadToPostgres.py - BEZ Step Functions/LocalStack Lambda deploy-a.

Poziva lambda_handler() direktno u ovom procesu, protiv lokalnog Postgres kontejnera
(docker-compose.local.yml) i lokalno postavljenog "gold report" JSON fajla u LocalStack S3
(ili, ako nemate S3 spreman, možete privremeno da monkey-patch-ujete _load_gold_report
da čita sa diska - vidi komentar ispod).

Preduslovi:
  1. docker compose -f docker-compose.local.yml up -d   (Postgres mora da radi na localhost:5432)
  2. Gold bucket u LocalStack S3 mora imati report.json na ključu koji prosledite (REPORT_KEY),
     ili prilagodite skriptu da čita results.json direktno sa diska (pogledajte niže).

Pokretanje:
  DB_HOST=localhost DB_PORT=5432 DB_NAME=gold_analytics DB_USER=analytics_user \
  DB_PASSWORD=analytics_pass GOLD_BUCKET_NAME=gold-layer-000000000000-dev \
  python3 test_load_to_postgres_local.py
"""
import json
import os
import sys

# --- Konfiguracija za lokalni test (default vrednosti odgovaraju docker-compose.local.yaml) ---
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "gold_analytics")
os.environ.setdefault("DB_USER", "analytics_user")
os.environ.setdefault("DB_PASSWORD", "analytics_pass")
os.environ.setdefault("GOLD_BUCKET_NAME", "gold-layer-000000000000-dev")

os.environ.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

sys.path.insert(0, "lambdas")  
from lambdas import loadToPostgres 

REPORT_KEY = "analytical_snapshots/date=2026-05-25/report.json"

USE_LOCAL_JSON_FILE = True 
LOCAL_JSON_PATH = "results.json"

if USE_LOCAL_JSON_FILE:
    def _load_gold_report_from_disk(report_key):
        with open(LOCAL_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)

    loadToPostgres._load_gold_report = _load_gold_report_from_disk

    import awswrangler as wr

    def _empty_read_parquet(*args, **kwargs):
        import pandas as pd
        return pd.DataFrame(columns=["date", "platform", "total_users", "new_users", "data_quality_score"])

    wr.s3.read_parquet = _empty_read_parquet

if __name__ == "__main__":
    event = {"ReportKey": REPORT_KEY}
    result = loadToPostgres.lambda_handler(event, None)
    print("\n[RESULT]", json.dumps(result, indent=2))
    print("\nProverite tabele u Postgres-u, npr:")
    print("  docker exec -it postgres-local psql -U analytics_user -d gold_analytics "
          "-c 'SELECT * FROM hn_daily_counts;'")