#!/bin/bash

STACK_NAME="cloud-computing-pipeline-prod"
REGION="eu-north-1"

aws cloudformation deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --region $REGION \
  --parameter-overrides Environment=prod