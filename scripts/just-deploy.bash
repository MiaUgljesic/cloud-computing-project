#!/bin/bash
set -e

STACK_NAME="cloud-computing-pipeline-prod"
REGION="eu-north-1"
CREDS_FILE=".aws-deploy-credentials"

if [ ! -f "$CREDS_FILE" ]; then
  echo "[ERROR] $CREDS_FILE not found. Run deploy-aws.bash first (it generates and saves credentials)."
  exit 1
fi
source "$CREDS_FILE"

aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides \
      Environment=prod \
      DBPassword="$DB_PASSWORD" \
      SupersetAdminPassword="$SUPERSET_ADMIN_PASSWORD" \
      SupersetSecretKey="$SUPERSET_SECRET_KEY" \
      AllowedSupersetCidr="$ALLOWED_SUPERSET_CIDR"