#!/bin/bash

# ==================== CONFIGURATION ====================
CODE_BUCKET="lambda-code-bucket-174435304073"
STACK_NAME="cloud-computing-pipeline-prod"
REGION="eu-north-1"
# =======================================================

echo "[INFO] 1. Creating S3 bucket for code deployment (Artifact Bucket)..."
aws s3 mb s3://$CODE_BUCKET --region $REGION || echo "[INFO] Bucket might already exist, proceeding..."

echo "[INFO] 2. Packaging Lambda functions into ZIP archives..."
# Navigate into the lambdas directory, package them, and output the ZIPs to the project root
cd lambdas
zip ../hackerNewsIngest.zip hackerNewsIngest.py
zip ../twitterIngest.zip twitterIngest.py
zip ../transform.zip transform.py
zip ../aggregate.zip aggregate.py
cd ..

echo "[INFO] 3. Uploading ZIP archives to AWS S3..."
aws s3 cp hackerNewsIngest.zip s3://$CODE_BUCKET/
aws s3 cp twitterIngest.zip s3://$CODE_BUCKET/
aws s3 cp transform.zip s3://$CODE_BUCKET/
aws s3 cp aggregate.zip s3://$CODE_BUCKET/

echo "[INFO] 4. Executing CloudFormation Deployment..."
# This command deploys your template.yaml file to AWS to provision the infrastructure resources
aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides Environment=prod

echo "[SUCCESS] Pipeline infrastructure successfully provisioned and deployed to AWS!"