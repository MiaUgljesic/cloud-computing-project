#!/bin/bash
set -e

echo "[INFO] Cleaning up old application zip files..."
rm -f hackerNewsIngest.zip twitterIngest.zip transform.zip aggregate.zip loadToPostgres.zip

# 1. FAST PACKAGING OF APPLICATION CODE
echo "[INFO] Packaging Lambda functions..."
cd lambdas
zip ../hackerNewsIngest.zip hackerNewsIngest.py
zip ../twitterIngest.zip twitterIngest.py
zip ../transform.zip transform.py
zip ../aggregate.zip aggregate.py
zip ../loadToPostgres.zip loadToPostgres.py
zip ../notification.zip notification.py
cd ..

# 2. QUICK UPLOAD TO S3
echo "[INFO] Uploading application code to S3..."
aws --endpoint-url=http://localhost:4566 s3 cp aggregate.zip s3://lambda-code-bucket/aggregate.zip
aws --endpoint-url=http://localhost:4566 s3 cp transform.zip s3://lambda-code-bucket/transform.zip
aws --endpoint-url=http://localhost:4566 s3 cp hackerNewsIngest.zip s3://lambda-code-bucket/hackerNewsIngest.zip 
aws --endpoint-url=http://localhost:4566 s3 cp twitterIngest.zip s3://lambda-code-bucket/twitterIngest.zip
aws --endpoint-url=http://localhost:4566 s3 cp loadToPostgres.zip s3://lambda-code-bucket/loadToPostgres.zip
aws --endpoint-url=http://localhost:4566 s3 cp notification.zip s3://lambda-code-bucket/notification.zip

# 3. INSTANT LAMBDA FUNCTION CODE UPDATE
echo "[INFO] Refreshing Lambda function code..."

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name HackerNewsIngestFunction --s3-bucket lambda-code-bucket --s3-key hackerNewsIngest.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name TwitterIngestFunction --s3-bucket lambda-code-bucket --s3-key twitterIngest.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name TransformFunction --s3-bucket lambda-code-bucket --s3-key transform.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name AggregateFunction --s3-bucket lambda-code-bucket --s3-key aggregate.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name LoadToPostgresFunction --s3-bucket lambda-code-bucket --s3-key loadToPostgres.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name NotificationFunction --s3-bucket lambda-code-bucket --s3-key notification.zip

# Clean up local zip archives post-deployment
rm -f hackerNewsIngest.zip twitterIngest.zip transform.zip aggregate.zip loadToPostgres.zip notification.zip

echo "[SUCCESS] Lambda function code has been successfully refreshed!"