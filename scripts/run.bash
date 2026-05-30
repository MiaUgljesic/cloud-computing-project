#!/bin/bash

aws --endpoint-url=http://localhost:4566 stepfunctions start-execution \
               --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:medallion-pipeline-orchestrator-dev