#!/bin/bash
set -e

# ==================== CONFIGURATION ====================
CODE_BUCKET="lambda-code-bucket-174435304073"
STACK_NAME="cloud-computing-pipeline-prod"
REGION="eu-north-1"
CREDS_FILE=".aws-deploy-credentials"   # gitignore this file!
ENV_FILE=".env"
# =======================================================

# ---- Učitavanje osetljivih podataka iz .env fajla ----
if [ -f "$ENV_FILE" ]; then
  echo "[INFO] Loading configuration from $ENV_FILE"
  # Učitavamo .env i automatski eksportujemo sve varijable u bash okruženje
  set -a
  source "$ENV_FILE"
  set +a
else
  echo "[ERROR] $ENV_FILE file not found! Please create it before deploying."
  exit 1
fi

# Provera da li su ključne varijable definisane
if [ -z "$DB_PASSWORD" ] || [ -z "$DISCORD_WEBHOOK_URL" ]; then
  echo "[ERROR] Required variables (DB_PASSWORD, DISCORD_WEBHOOK_URL) are missing in $ENV_FILE"
  exit 1
fi

# ---- Secrets: use env var if provided, otherwise generate and remember ----
# Re-using the same CREDS_FILE across redeploys keeps the DB/Superset password
# stable, so you don't lock yourself out of Postgres or Superset admin on redeploy.
if [ -f "$CREDS_FILE" ]; then
  echo "[INFO] Loading previously generated credentials from $CREDS_FILE"
  source "$CREDS_FILE"
fi

DB_PASSWORD="${DB_PASSWORD:-$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 20)}"
SUPERSET_ADMIN_PASSWORD="${SUPERSET_ADMIN_PASSWORD:-$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 20)}"
SUPERSET_SECRET_KEY="${SUPERSET_SECRET_KEY:-$(openssl rand -base64 32)}"

# Restrict this to your own IP in production, e.g. ALLOWED_SUPERSET_CIDR="203.0.113.4/32"
ALLOWED_SUPERSET_CIDR="${ALLOWED_SUPERSET_CIDR:-0.0.0.0/0}"

cat > "$CREDS_FILE" <<EOF
DB_PASSWORD="$DB_PASSWORD"
SUPERSET_ADMIN_PASSWORD="$SUPERSET_ADMIN_PASSWORD"
SUPERSET_SECRET_KEY="$SUPERSET_SECRET_KEY"
ALLOWED_SUPERSET_CIDR="$ALLOWED_SUPERSET_CIDR"
EOF
chmod 600 "$CREDS_FILE"

echo "[INFO] 1. Creating S3 bucket for code deployment (Artifact Bucket)..."
aws s3 mb s3://$CODE_BUCKET --region $REGION || echo "[INFO] Bucket might already exist, proceeding..."

echo "[INFO] 2. Packaging Lambda functions into ZIP archives..."
# Navigate into the lambdas directory, package them, and output the ZIPs to the project root
cd lambdas
zip -j ../hackerNewsIngest.zip hackerNewsIngest.py
zip -j ../twitterIngest.zip twitterIngest.py
zip -j ../transform.zip transform.py
zip -j ../aggregate.zip aggregate.py
zip -j ../loadToPostgres.zip loadToPostgres.py
zip -j ../notification.zip notification.py
cd ..

echo "[INFO] 3. Uploading ZIP archives to AWS S3..."
aws s3 cp hackerNewsIngest.zip s3://$CODE_BUCKET/
aws s3 cp twitterIngest.zip s3://$CODE_BUCKET/
aws s3 cp transform.zip s3://$CODE_BUCKET/
aws s3 cp aggregate.zip s3://$CODE_BUCKET/
aws s3 cp loadToPostgres.zip s3://$CODE_BUCKET/
aws s3 cp notification.zip s3://$CODE_BUCKET/

echo "[INFO] 4. Executing CloudFormation Deployment..."

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
      AllowedSupersetCidr="$ALLOWED_SUPERSET_CIDR" \
      DiscordWebhookUrl="$DISCORD_WEBHOOK_URL"

echo ""
echo "[SUCCESS] Pipeline infrastructure successfully provisioned and deployed to AWS!"
echo "[INFO] Credentials saved to ./$CREDS_FILE - add this file to .gitignore, don't commit it."
echo "[INFO] Superset URL: check the SupersetURL output below (or 'aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION --query \"Stacks[0].Outputs\"')"