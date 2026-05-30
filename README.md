# Hacker News Data Pipeline - Medallion Architecture

A production-grade data pipeline using AWS Lambda and Step Functions to implement the **Medallion Architecture** (Bronze → Silver → Gold layers) for processing Hacker News and Twitter data. Optimized for **LocalStack** containerized development.

---

## 📋 Architecture Overview

### Medallion Pattern (Three-Layer Data Lake)

```
┌─────────────────────────────────────────────────────────────────┐
│  STEP FUNCTIONS ORCHESTRATOR (State Machine)                     │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ ┌──────────────┐      ┌─────────────────────────────────┐ │ │
│  │ │  Parallel    │      │ 1. HN Ingestion + Twitter       │ │ │
│  │ │  Branches    ├─────▶│    Ingestion (parallel)        │ │ │
│  │ │              │      │ Returns file_keys              │ │ │
│  │ └──────────────┘      └─────────────────────────────────┘ │ │
│  │         │                                                   │ │
│  │         └──────────────┬──────────────────────────────────┘ │ │
│  │                        │                                     │ │
│  │  ┌────────────────────▼─────────────────────────────────┐  │ │
│  │  │  2. Transform Lambda                                │  │ │
│  │  │     (uses both file_keys from step 1)              │  │ │
│  │  │     Returns HN + Twitter Silver file_keys         │  │ │
│  │  └────────────────────┬─────────────────────────────────┘  │ │
│  │                       │                                      │ │
│  │  ┌────────────────────▼─────────────────────────────────┐  │ │
│  │  │  3. Aggregate Lambda                                │  │ │
│  │  │     (uses HN Silver file_key)                       │  │ │
│  │  │     Returns Gold metrics file_key                  │  │ │
│  │  └────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                            │
                ┌───────────┼───────────┐
                │           │           │
        ┌──────▼──────┐ ┌──▼──────┐ ┌─▼──────────┐
        │ BRONZE LAYER │ │ S3 SILVER  │ │ S3 GOLD    │
        │   (Raw Data)  │ │ (Cleaned)  │ │(Analyzed)  │
        └───────────────┘ └────────────┘ └────────────┘
            HN + Twitter      Transformed      Metrics &
            Raw JSON          & Normalized     Aggregates
```

---

## 🔑 **template-local.yaml** - The Infrastructure Blueprint

This is the **most critical file** - it defines the entire AWS infrastructure as code. It orchestrates every component: VPC, security groups, S3 buckets, Lambda functions, IAM roles, and the Step Functions state machine.

### Key Sections:

#### 1. **VPC & Networking**
```yaml
DataPipelineVPC:
  Type: AWS::EC2::VPC
  CidrBlock: 10.0.0.0/16
  
PrivateSubnet:
  Type: AWS::EC2::Subnet
  CidrBlock: 10.0.1.0/24
```
- Creates isolated network for Lambda functions
- Lambda runs inside VPC for security and testing

#### 2. **S3 Buckets (Three Layers)**

```yaml
BronzeLayerBucket:        # Raw ingested data
  - Hacker News raw API responses
  - Twitter raw data
  - No transformations

SilverLayerBucket:        # Cleaned & normalized data
  - HN stories (filtered by type)
  - Tweets with extracted metadata
  - HTML cleaned, deduplicated

GoldLayerBucket:          # Aggregated analytics
  - Metrics reports
  - Business intelligence
  - Top stories, averages, insights
```

Each bucket has:
- ✅ Versioning enabled (track data lineage)
- ✅ Public access blocked (security)
- ✅ Explicit naming: `{layer}-layer-{AccountId}-{Environment}`

#### 3. **IAM Roles & Policies**

**LambdaExecutionRole:**
```yaml
Policies:
  - S3 bucket-level permissions (Bronze/Silver/Gold)
  - CloudWatch Logs (debug/monitoring)
  - VPC execution role (ENI management)
```
Principle: **Least privilege** - functions only access their specific layer buckets.

**StateMachineExecutionRole:**
```yaml
Permissions:
  - InvokeFunction on all 4 Lambda functions
  - No direct S3 access (only calls Lambdas)
```

#### 4. **Step Functions State Machine** (The Orchestrator)

```yaml
MedallionPipelineStateMachine:
  States:
    Parallel_Ingestion:          # Step 1: Run in parallel
      ├─ IngestHackerNews        → Returns bronze file_key
      └─ IngestTwitter           → Returns bronze file_key
                ↓
    TransformToSilver:           # Step 2: Combined transformation
      Parameters:
        HNFileKey: $.IngestResults[0].body.file_key
        TwitterFileKey: $.IngestResults[1].body.file_key
                ↓
    AggregateToGold:             # Step 3: Analytics & metrics
      Parameters:
        SilverFileKey: $.TransformResult.body.file_key
```

**Critical Feature: Parameter Passing**
- Step 1 outputs → Step 2 inputs via `Parameters` section
- Step 2 outputs → Step 3 inputs
- This chains the pipeline: `Bronze → Silver → Gold`

#### 5. **Lambda Functions**

Each Lambda is defined with:
- **Handler**: Entrypoint function (e.g., `hackerNewsIngest.lambda_handler`)
- **Runtime**: Python 3.11
- **Code**: S3 location (bucket: `lambda-code-bucket`)
- **Environment variables**: Bucket names injected at deploy time
- **Layers**: Pandas/NumPy layer attached (for Transform & Aggregate)
- **VPC Config**: Runs inside the PrivateSubnet

```yaml
HackerNewsIngestFunction:
  Code:
    S3Bucket: lambda-code-bucket
    S3Key: hackerNewsIngest.zip
  Environment:
    BRONZE_BUCKET_NAME: !Ref BronzeLayerBucket  # Resolved at deploy
```

#### 6. **Lambda Layer (PandasWranglerLayer)**

```yaml
PandasWranglerLayer:
  - Packages: pandas, numpy, awswrangler (pre-installed)
  - Attached to: Transform & Aggregate functions
  - Saves time: Functions don't reinstall dependencies
```

### Why template-local.yaml is Important:

1. **Single Source of Truth**: All infrastructure defined in one YAML
2. **Reproducible**: Run same template → identical infrastructure
3. **LocalStack Compatible**: Uses `--endpoint-url=http://localhost:4566` 
4. **Parameter-Driven Pipeline**: State Machine passes data between stages
5. **Immutable Buckets**: Each layer writes to its own S3 bucket
6. **Auto-Generated Names**: Uses CloudFormation functions (`!Ref`, `!Sub`) for dynamic naming

---

## 🐍 Lambda Functions

### 1. **hackerNewsIngest.py** - Bronze Layer (Ingestion)

**Purpose**: Fetch raw data from public Hacker News API

```python
def lambda_handler(event, context):
    # 1. Fetch top 5 story IDs from HN API
    top_stories_ids = fetch_from_api("https://hacker-news.firebaseio.com/v0/topstories.json")
    
    # 2. For each ID, fetch full story data
    fetched_items = []
    for item_id in top_stories_ids[:5]:
        item_data = fetch_item(item_id)
        fetched_items.append(item_data)
    
    # 3. Upload raw JSON to Bronze Layer
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=f"hacker_news/raw_stories_{timestamp}.json",
        Body=json.dumps(fetched_items)
    )
    
    # 4. Return file_key for next step
    return {
        'statusCode': 200,
        'body': {
            'file_key': s3_key,
            'bucket': BRONZE_BUCKET
        }
    }
```

**Output Format**:
```json
{
  "statusCode": 200,
  "body": {
    "message": "Ingestion successful",
    "file_key": "hacker_news/raw_stories_20260522_175200.json",
    "bucket": "bronze-layer-123456-dev"
  }
}
```

### 2. **twitterIngest.py** - Bronze Layer (Twitter)

**Purpose**: Fetch Twitter data (or simulated data)

Similar structure to HN, but:
- Fetches tweets instead of stories
- Returns `file_key` to Bronze bucket
- Parallel with HN (both run simultaneously)

### 3. **transform.py** - Silver Layer (Cleaning)

**Purpose**: Clean and normalize both HN and Twitter data

```python
def lambda_handler(event, context):
    # Receives both file_keys from Step 1
    hn_file_key = event.get('HNFileKey')
    twitter_file_key = event.get('TwitterFileKey')
    
    # --- Process HN ---
    hn_data = s3.get_object(Bucket=BRONZE_BUCKET, Key=hn_file_key)
    cleaned_stories = []
    for item in hn_data:
        if item.get("type") == "story":
            cleaned_stories.append({
                "id": item.get("id"),
                "title": item.get("title"),
                "score": item.get("score", 0),
                "text": clean_html(item.get("text", "")),
                "url": item.get("url", "")
            })
    
    silver_hn_key = f"storys/silver_hn_{timestamp}.json"
    s3.put_object(Bucket=SILVER_BUCKET, Key=silver_hn_key, Body=json.dumps(cleaned_stories))
    
    # --- Process Twitter ---
    twitter_data = s3.get_object(Bucket=BRONZE_BUCKET, Key=twitter_file_key)
    cleaned_tweets = []
    for tweet in twitter_data:
        cleaned_tweets.append({
            "id": tweet.get("tweet_id"),
            "author": tweet.get("user"),
            "clean_text": clean_html(tweet.get("text")),
            "popularity_score": tweet.get("retweet_count", 0) + tweet.get("favorite_count", 0)
        })
    
    twitter_silver_key = f"tweets/silver_tweets_{timestamp}.json"
    s3.put_object(Bucket=SILVER_BUCKET, Key=twitter_silver_key, Body=json.dumps(cleaned_tweets))
    
    # Return both keys for next step
    return {
        'statusCode': 200,
        'body': {
            'file_key': silver_hn_key,        # HN key for Gold layer
            'twitter_file_key': twitter_silver_key
        }
    }
```

**Transformations Applied**:
- ✅ Filter: Keep only stories (exclude jobs, polls, comments)
- ✅ Clean HTML: Remove `<tag>` markup from text
- ✅ Extract: Pull only relevant fields
- ✅ Normalize: Consistent field names and types
- ✅ Partition: Write to separate prefixes (`storys/`, `tweets/`)

### 4. **aggregate.py** - Gold Layer (Analytics)

**Purpose**: Generate business metrics and insights

```python
def lambda_handler(event, context):
    # Receives Silver HN file_key from Step 2
    file_key = event.get('SilverFileKey')
    
    # Read cleaned stories
    silver_data = s3.get_object(Bucket=SILVER_BUCKET, Key=file_key)
    
    # Calculate metrics
    total_stories = len(silver_data)
    average_score = sum(s.get("score", 0) for s in silver_data) / total_stories
    top_story = max(silver_data, key=lambda x: x.get("score", 0))
    
    # Build metrics report
    metrics = {
        "aggregation_timestamp": datetime.now().isoformat(),
        "source_file": file_key,
        "metrics": {
            "total_stories_analyzed": total_stories,
            "average_story_score": round(average_score, 2),
            "highest_rated_story": top_story
        }
    }
    
    # Save to Gold Layer
    gold_key = f"metrics/gold_metrics_{timestamp}.json"
    s3.put_object(Bucket=GOLD_BUCKET, Key=gold_key, Body=json.dumps(metrics))
    
    return {
        'statusCode': 200,
        'body': {
            'message': 'Aggregation successful',
            'file_key': gold_key
        }
    }
```

**Output Analytics**:
```json
{
  "aggregation_timestamp": "2026-05-22T17:52:10.123456",
  "source_file_processed": "storys/silver_hn_20260522_175200.json",
  "metrics": {
    "total_stories_analyzed": 5,
    "average_story_score": 342.6,
    "highest_rated_story": {
      "id": 35431234,
      "title": "Ask HN: Best Cloud Architecture for ML?",
      "score": 512,
      "by": "techninja",
      "url": "https://news.ycombinator.com/item?id=35431234"
    }
  }
}
```

---

## 📝 Scripts - Deployment Automation

### 1. **deploy.bash** - Spin Up Infrastructure

```bash
aws --endpoint-url=http://localhost:4566 cloudformation deploy \
    --template-file template-local.yaml \
    --stack-name medallion-pipeline \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides Environment=dev
```

**What It Does**:
1. Reads `template-local.yaml`
2. Creates CloudFormation stack named `medallion-pipeline`
3. Provisions all resources: VPC, subnets, security groups, S3 buckets, Lambda functions, state machine
4. Injects environment variables into Lambdas
5. Registers state machine

**Output**: CloudFormation stack with all components ready

---

### 2. **build_layer.bash** - Build Python Dependencies

```bash
# 1. Create layer structure
mkdir -p ./layer_temp/python
pip install --target ./layer_temp/python pandas numpy awswrangler

# 2. AGGRESSIVE OPTIMIZATION - Remove 50%+ of bloat
find . -type d -name "tests" -exec rm -rf {} +
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type d -name "*.dist-info" -exec rm -rf {} +

# 3. ZIP and upload to S3
zip -r pandas_layer.zip python/
aws s3 cp pandas_layer.zip s3://lambda-code-bucket/pandas_layer.zip

# 4. Publish layer version
LAYER_ARN=$(aws lambda publish-layer-version \
    --layer-name AWSSDKPandas-Python311 \
    --content S3Bucket=lambda-code-bucket,S3Key=pandas_layer.zip \
    --compatible-runtimes python3.11 \
    --query 'LayerVersionArn' --output text)

echo $LAYER_ARN > .last_layer_arn
```

**Why This Matters**:
- ✅ Pandas + NumPy + awswrangler are **heavy** (100MB+)
- ✅ Optimize by removing tests, cache, metadata: ~50MB
- ✅ Layer is **versioned** - old functions keep working
- ✅ New functions automatically get latest layer

**Output**: `.last_layer_arn` file with reference to published layer

---

### 3. **upload_functions.bash** - Deploy Lambda Code

```bash
# 1. ZIP only your application code (small)
zip hackerNewsIngest.zip lambdas/hackerNewsIngest.py
zip transform.zip lambdas/transform.py
zip aggregate.zip lambdas/aggregate.py

# 2. Upload ZIPs to S3
aws s3 cp transform.zip s3://lambda-code-bucket/transform.zip

# 3. UPDATE existing Lambda functions
aws lambda update-function-code \
    --function-name TransformFunction \
    --s3-bucket lambda-code-bucket \
    --s3-key transform.zip

# 4. RE-ATTACH layer (because layer wasn't updated)
LAYER_ARN=$(cat .last_layer_arn)
aws lambda update-function-configuration \
    --function-name TransformFunction \
    --layers "$LAYER_ARN"
```

**Optimization**:
- Only uploads **application code** (5-10 KB), not dependencies
- Reuses layer from step 2 (no reinstall)
- Update complete in seconds (vs minutes)

---

### 4. **run.bash** - Execute Pipeline

```bash
aws --endpoint-url=http://localhost:4566 stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:medallion-pipeline-orchestrator-dev
```

**What It Does**:
1. Triggers Step Functions state machine
2. State machine orchestrates all 3 steps in order:
   - Step 1: Parallel HN + Twitter ingestion
   - Step 2: Combined transformation (uses output from Step 1)
   - Step 3: Analytics & metrics (uses output from Step 2)

**Returns**: Execution ARN for monitoring status

---

## 🚀 Complete Workflow

### Step 1: Initial Setup
```bash
# Deploy all infrastructure once
./scripts/deploy.bash

# Build pandas layer once
./scripts/build_layer.bash
```

### Step 2: Iterative Development
```bash
# (Edit lambda code)

# Quick upload (30 seconds)
./scripts/upload_functions.bash

# Run pipeline
./scripts/run.bash
```

### Data Flow:
```
┌─────────────────────────────────────────────────────────┐
│ 1. run.bash triggers state machine                       │
├─────────────────────────────────────────────────────────┤
│ 2. Parallel:                                             │
│    - hackerNewsIngest.py  → hacker_news/raw_*.json      │
│    - twitterIngest.py     → twitter/raw_*.json          │
│                                                          │
│ 3. transform.py receives both file_keys                 │
│    - Cleans HN stories   → storys/silver_hn_*.json      │
│    - Cleans tweets       → tweets/silver_tweets_*.json  │
│                                                          │
│ 4. aggregate.py receives HN silver key                  │
│    - Calculates metrics  → metrics/gold_metrics_*.json  │
├─────────────────────────────────────────────────────────┤
│ OUTPUT: 3 S3 buckets with layered data                  │
└─────────────────────────────────────────────────────────┘
```

---

## 📊 Example Data at Each Layer

### Bronze Layer (Raw)
```json
// s3://bronze-layer-123456-dev/hacker_news/raw_stories_20260522_175200.json
[
  {
    "by": "pg",
    "descendants": 2341,
    "id": 35600000,
    "kids": [...],
    "score": 1200,
    "time": 1684776612,
    "title": "Show HN: Building Production ML with LocalStack",
    "type": "story",
    "url": "https://..."
  },
  ...
]
```

### Silver Layer (Cleaned)
```json
// s3://silver-layer-123456-dev/storys/silver_hn_20260522_175200.json
[
  {
    "id": 35600000,
    "by": "pg",
    "time": 1684776612,
    "score": 1200,
    "title": "Show HN: Building Production ML with LocalStack",
    "text": "Building production ML systems...",
    "url": "https://..."
  },
  ...
]
```

### Gold Layer (Metrics)
```json
// s3://gold-layer-123456-dev/metrics/gold_metrics_20260522_175200.json
{
  "aggregation_timestamp": "2026-05-22T17:52:10.123456",
  "source_file_processed": "storys/silver_hn_20260522_175200.json",
  "metrics": {
    "total_stories_analyzed": 5,
    "average_story_score": 342.6,
    "highest_rated_story": {
      "id": 35600000,
      "title": "Show HN: Building Production ML with LocalStack",
      "score": 1200,
      "by": "pg"
    }
  }
}
```

---

## 🔧 Error Handling & Debugging

### Lambda Functions Use Early Failure Pattern:
```python
try:
    # Process data
except Exception as e:
    error_msg = f"[ERROR] Transformation failed: {str(e)}"
    print(error_msg)
    raise e  # ← Forces Step Functions to catch and retry/fail
```

**Why `raise e`?**
- If Lambdas caught exceptions and returned 200, Step Functions thinks execution succeeded
- Raising the exception allows Step Functions to:
  - Trigger retry logic
  - Execute failure handlers
  - Send proper alerts

### Monitoring:
```bash
# Check CloudWatch logs
aws logs tail /aws/lambda/TransformFunction --follow --endpoint-url=http://localhost:4566

# Check Step Functions execution status
aws stepfunctions describe-execution \
    --execution-arn arn:aws:states:us-east-1:000000000000:execution:medallion-pipeline-orchestrator-dev:execution-id \
    --endpoint-url=http://localhost:4566
```

---

## 🎯 Key Takeaways

| Component | Purpose | Defined In |
|-----------|---------|-----------|
| **template-local.yaml** | Infrastructure-as-Code: All AWS resources (VPC, buckets, roles, Lambdas, state machine) | YAML |
| **State Machine** | Orchestrates the pipeline: coordinates which Lambdas run and when | template-local.yaml (JSON) |
| **Lambda Functions** | Business logic: ingest, transform, aggregate data | Python files |
| **Buckets** | Data storage: Bronze (raw) → Silver (cleaned) → Gold (insights) | template-local.yaml |
| **deploy.bash** | One-time infrastructure provisioning | CloudFormation |
| **build_layer.bash** | One-time dependencies (pandas, numpy) | Lambda Layer |
| **upload_functions.bash** | Iterative code updates (dev-test cycle) | Lambda Update API |
| **run.bash** | Execute the pipeline | Step Functions |

---

## 📦 Project Structure

```
cloud-computing-project/
├── template-local.yaml              # ⭐ MAIN: Infrastructure blueprint
├── template.yaml                    # Production version (AWS)
├── README_COMPREHENSIVE.md          # This file
├── serverless.yml                   # Alternative: Serverless Framework config
│
├── lambdas/
│   ├── hackerNewsIngest.py         # Step 1: Fetch HN data → Bronze
│   ├── twitterIngest.py             # Step 1: Fetch Twitter data → Bronze
│   ├── transform.py                 # Step 2: Clean both → Silver
│   └── aggregate.py                 # Step 3: Metrics → Gold
│
└── scripts/
    ├── deploy.bash                  # Initialize infrastructure
    ├── build_layer.bash             # Build Python dependencies
    ├── upload_functions.bash        # Upload Lambda code
    └── run.bash                      # Trigger pipeline execution
```

---

## 🚁 Quick Start

```bash
# 1. Deploy infrastructure (first time only)
./scripts/deploy.bash

# 2. Build dependencies layer (first time only)
./scripts/build_layer.bash

# 3. Upload your Lambda code
./scripts/upload_functions.bash

# 4. Run the pipeline
./scripts/run.bash

# 5. Monitor
# CloudWatch → Lambda logs
# S3 → Check data in Bronze/Silver/Gold buckets
```

Done! Your data pipeline is running. ✅
