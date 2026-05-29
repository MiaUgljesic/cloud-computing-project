import json
import urllib.request
import boto3
import os
import datetime

s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

def fetch_item(item_id):
    """Pomoćna funkcija za povlačenje pojedinačnog item-a sa HN API-ja"""
    url = f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None

def lambda_handler(event, context):
    print("[INFO] Pokretanje Hacker News data ingestion pipeline-a...")
    
    try:
        # API endpointovi za različite tipove postova (Zadovoljavanje Zahteva 1.1)
        endpoints = {
            "story": "https://hacker-news.firebaseio.com/v0/newstories.json",
            "ask": "https://hacker-news.firebaseio.com/v0/askstories.json",
            "job": "https://hacker-news.firebaseio.com/v0/jobstories.json",
            "show": "https://hacker-news.firebaseio.com/v0/showstories.json"
        }
        
        fetched_items = []
        comment_ids = []
        
        # 1. Povlačenje svih tipova postova (ograničeno na 15 po tipu zbog Lambda Timeout-a)
        for post_type, url in endpoints.items():
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                ids = json.loads(response.read().decode())
                
            for item_id in ids[:15]:
                item = fetch_item(item_id)
                if item:
                    fetched_items.append(item)
                    # Skupljamo `kids` (komentare) kako bismo ispoštovali i povlačenje 'comments' tipa
                    if "kids" in item:
                        comment_ids.extend(item["kids"][:2])

        # 2. Povlačenje samih komentara
        for cid in comment_ids[:20]:
            comment = fetch_item(cid)
            if comment:
                fetched_items.append(comment)

        # 3. Slanje na S3 (Bronze)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"hacker_news/raw_stories_{timestamp}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(fetched_items, indent=4)
        )
        
        success_message = f"[SUCCESS] Uspesno prikupljeno {len(fetched_items)} HN objekata (stories, asks, jobs, comments)."
        print(success_message)
        
        return {
            'statusCode': 200,
            'body': {
                'message': success_message,
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        print(f"[ERROR] Ingestion failed: {str(e)}")
        raise e