#!/bin/bash
set -e

echo "[INFO] Čišćenje starih instalacija i privremenih ZIP arhiva za Layer..."
rm -rf ./layer_temp
rm -f pandas_layer.zip

# 1. KREIRANJE ISPRAVNE STRUKTURE ZA LAYER
echo "[INFO] Preuzimanje optimizovanih biblioteka (pandas, numpy, awswrangler)..."
mkdir -p ./layer_temp/python

# Instaliramo pakete sa flagom --no-cache-dir da izbegnemo smeće
pip install --no-cache-dir --target ./layer_temp/python pandas numpy awswrangler

# 2. RADIKALNO ČIŠĆENJE (Smanjuje otpakovanu veličinu za više od 50%)
echo "[INFO] Optimizacija i brisanje nepotrebnih fajlova iz biblioteka (testovi, cache, binarne distorzije)..."
cd layer_temp/python

# Brišemo kompletne test foldere koje biblioteke vuku sa sobom
find . -type d -name "tests" -exec rm -rf {} +
find . -type d -name "test" -exec rm -rf {} +

# Brišemo prekompajlirane Python fajlove (__pycache__)
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
find . -type f -name "*.pyo" -delete

# Brišemo .egg-info i dist-info meta-podatke koji nisu potrebni za izvršavanje koda
find . -type d -name "*.dist-info" -exec rm -rf {} +
find . -type d -name "*.egg-info" -exec rm -rf {} +

# Vraćamo se nazad u korenski folder
cd ../..

echo "[INFO] Pakovanje pandas_layer.zip sa ispravnom 'python/' strukturom..."
cd layer_temp
zip -q -r ../pandas_layer.zip python/
cd ..
rm -rf ./layer_temp

# Provera veličine spakovanog fajla
ZIP_SIZE=$(du -sh pandas_layer.zip | cut -f1)
echo "[INFO] Uspešno kreiran ZIP. Veličina arhive na disku: $ZIP_SIZE"

# 3. SLANJE NA S3 BUCKET
echo "[INFO] Slanje pandas_layer.zip na MiniStack S3..."
aws --endpoint-url=http://localhost:4566 s3 cp pandas_layer.zip s3://lambda-code-bucket/pandas_layer.zip

# 4. REGISTRACIJA LAYER-A NA AWS LAMBDI
echo "[INFO] Publikovanje nove verzije Layer-a na MiniStack-u..."
LAYER_ARN=$(aws --endpoint-url=http://localhost:4566 lambda publish-layer-version \
    --layer-name AWSSDKPandas-Python311 \
    --description "Lokalni pandas, numpy i awswrangler layer" \
    --content S3Bucket=lambda-code-bucket,S3Key=pandas_layer.zip \
    --compatible-runtimes python3.11 \
    --query 'LayerVersionArn' --output text)

# Smeštamo ARN u privremeni fajl kako bi upload_functions.bash mogao da ga pročita bez ponovnog skidanja
echo "$LAYER_ARN" > .last_layer_arn

echo "--------------------------------------------------------"
echo "[SUCCESS] Layer je uspešno optimizovan, izgrađen i registrovan!"
echo "[INFO] Trenutni Layer ARN: $LAYER_ARN"
echo "--------------------------------------------------------"