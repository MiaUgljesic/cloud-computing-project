import json
import boto3
import os
import datetime
import random

s3 = boto3.client('s3')

# Izvlačimo naziv bucketa iz environment varijabli koje smo podesili
BUCKET_NAME = os.environ.get("BRONZE_BUCKET_NAME", "bronze-layer-fallback")

def lambda_handler(event, context):
    print("[INFO] Pokretanje X (Twitter) data ingestion pipeline-a...")
    
    try:
        # Simulacija produkcionog dataseta (generisanje mockup tvitova bazirano na realnim datasetovima)
        # U realnom scenariju ovde možete učitati lokalni CSV/JSON sa Bitcoin/Tehnološkim tvitovima
        teme = ["Bitcoin", "AI", "CloudComputing", "Serverless", "Python"]
        mock_tweets = []
        
        # Generišemo 15 tvitova za bolji uzorak
        for i in range(15):
            mock_tweets.append({
                "tweet_id": str(random.randint(1000000000, 9999999999)),
                "created_at": (datetime.datetime.now() - datetime.timedelta(days=1)).isoformat() + "Z",
                "user": f"tech_user_{random.randint(1, 100)}",
                "text": f"Sjajan dan za učenje o {random.choice(teme)} i AWS Lambda funkcijama! #cloud",
                "retweet_count": random.randint(0, 50),
                "favorite_count": random.randint(0, 150),
                "followers_count": random.randint(10, 50000),  # KLJUČNO: Dodato za Gold layer (Top 10 lista)
                "is_verified": random.choice([True, False]),   # KLJUČNO: Dodato za Silver layer (Šema)
                "lang": "en"
            })
            
        # Kreiranje jedinstvenog ključa (particionisanje po izvoru: twitter/)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"twitter/raw_tweets_{timestamp}.json"
        
        # Upisivanje u Bronze layer u izvornom obliku (bez transformacija)
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(mock_tweets, indent=4)
        )
        
        print(f"[INFO] Uspešno sačuvani raw Twitter podaci na {BUCKET_NAME}/{s3_key}")
        
        # Vraćamo strukturirani odgovor za Step Function
        return {
            'statusCode': 200,
            'body': {
                'message': 'X Ingestion uspešan',
                'file_key': s3_key,
                'bucket': BUCKET_NAME
            }
        }
        
    except Exception as e:
        error_msg = f"[ERROR] X Ingestion neuspešan: {str(e)}"
        print(error_msg)
        raise e