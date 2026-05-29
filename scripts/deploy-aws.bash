#!/bin/bash

# ==================== KONFIGURACIJA ====================
CODE_BUCKET="lambda-code-bucket-174435304073" # !!! Stavi isto ime kao iz template.yaml !!!
STACK_NAME="cloud-computing-pipeline-prod"
REGION="eu-north-1"
# =======================================================

echo "[INFO] 1. Kreiranje S3 bucketa za kod (Artifact Bucket)..."
aws s3 mb s3://$CODE_BUCKET --region $REGION || echo "[INFO] Bucket verovatno već postoji, nastavljamo..."

echo "[INFO] 2. Pakovanje Lambda funkcija u ZIP arhive..."
# Ulazimo u folder sa lambdama, pakujemo ih i vraćamo zipove u koren projekta
cd lambdas
zip ../hackerNewsIngest.zip hackerNewsIngest.py
zip ../twitterIngest.zip twitterIngest.py
zip ../transform.zip transform.py
zip ../aggregate.zip aggregate.py
cd ..

echo "[INFO] 3. Upload ZIP arhiva na AWS S3..."
aws s3 cp hackerNewsIngest.zip s3://$CODE_BUCKET/
aws s3 cp twitterIngest.zip s3://$CODE_BUCKET/
aws s3 cp transform.zip s3://$CODE_BUCKET/
aws s3 cp aggregate.zip s3://$CODE_BUCKET/

echo "[INFO] 4. Pokretanje CloudFormation Deployment-a..."
# Ova komanda šalje tvoj template.yaml na AWS koji onda zida infrastrukturu
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides Environment=prod

echo "[USPEH] Sve je uspešno podignuto i postavljeno na AWS nalog!"