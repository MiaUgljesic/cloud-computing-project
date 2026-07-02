# lambdas/notification.py
import urllib.request
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Primljen error event iz Step Functions: {json.dumps(event)}")
    
    # URL povlačimo iz Environment varijabli koje definišemo u YAML-u
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    
    if not webhook_url or webhook_url == 'TVOJ_WEBHOOK_URL_OVDE':
        logger.error("Discord Webhook URL nije konfigurisan!")
        return {"statusCode": 400, "body": "Missing Webhook URL"}

    # Izvlačimo detalje o grešci iz Step Functions konteksta
    error_msg = "Nepoznata greška u pipeline-u."
    if "Cause" in event:
        try:
            # Često je 'Cause' stringifikovan JSON, pokušavamo da izvučemo 'errorMessage'
            cause_data = json.loads(event["Cause"])
            error_msg = cause_data.get("errorMessage", event["Cause"])
        except Exception:
            error_msg = event["Cause"]
    elif "Error" in event:
        error_msg = event["Error"]

    # Formatiramo lepu poruku za Discord channel
    discord_message = {
        "content": f" ! **Data Pipeline Error Alert!**\n **Error details:** `{error_msg}`"
    }
    
    req = urllib.request.Request(
        webhook_url, 
        data=json.dumps(discord_message).encode('utf-8'), 
        headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            logger.info("Notifikacija uspešno poslata na Discord.")
            return {"statusCode": 200, "body": "Notification sent"}
    except Exception as e:
        logger.error(f"Greška prilikom slanja na Discord: {str(e)}")
        raise e