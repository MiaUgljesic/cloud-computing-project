#!/bin/bash
set -e

echo "[INFO] Čišćenje starih aplikativnih zipova..."
rm -f hackerNewsIngest.zip twitterIngest.zip transform.zip aggregate.zip

# 1. BRZO PAKOVANJE SAMO TVOG KODA
echo "[INFO] Pakovanje Lambda funkcija"
cd lambdas
zip ../hackerNewsIngest.zip hackerNewsIngest.py
zip ../twitterIngest.zip twitterIngest.py
zip ../transform.zip transform.py
zip ../aggregate.zip aggregate.py
cd ..

# 2. BRZI UPLOAD NA S3
echo "[INFO] Slanje aplikativnog koda na S3..."
aws --endpoint-url=http://localhost:4566 s3 cp aggregate.zip s3://lambda-code-bucket/aggregate.zip
aws --endpoint-url=http://localhost:4566 s3 cp transform.zip s3://lambda-code-bucket/transform.zip
aws --endpoint-url=http://localhost:4566 s3 cp hackerNewsIngest.zip s3://lambda-code-bucket/hackerNewsIngest.zip 
aws --endpoint-url=http://localhost:4566 s3 cp twitterIngest.zip s3://lambda-code-bucket/twitterIngest.zip

# 3. INSTANTNO OSVEŽAVANJE FUNKCIJA NA LAMBDI
echo "[INFO] Osvežavanje koda"

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name HackerNewsIngestFunction --s3-bucket lambda-code-bucket --s3-key hackerNewsIngest.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name TwitterIngestFunction --s3-bucket lambda-code-bucket --s3-key twitterIngest.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name TransformFunction --s3-bucket lambda-code-bucket --s3-key transform.zip

aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name AggregateFunction --s3-bucket lambda-code-bucket --s3-key aggregate.zip

rm -f hackerNewsIngest.zip twitterIngest.zip transform.zip aggregate.zip

echo "[SUCCESS] Kod funkcija je osvežen!"