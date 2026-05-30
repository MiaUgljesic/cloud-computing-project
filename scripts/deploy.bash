#!/bin/bash

aws --endpoint-url=http://localhost:4566 cloudformation deploy \
               --template-file template-local.yaml \
               --stack-name medallion-pipeline \
               --capabilities CAPABILITY_IAM \
               --parameter-overrides Environment=dev