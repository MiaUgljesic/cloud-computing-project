aws lambda update-function-code \
    --function-name HackerNewsIngestFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key hackerNewsIngest.zip

aws lambda update-function-code \
    --function-name TwitterIngestFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key twitterIngest.zip

aws lambda update-function-code \
    --function-name TransformFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key transform.zip

aws lambda update-function-code \
    --function-name AggregateFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key aggregate.zip

aws lambda update-function-code \
    --function-name LoadToPostgresFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key loadToPostgres.zip

aws lambda update-function-code \
    --function-name NotificationFunction --s3-bucket lambda-code-bucket-174435304073 --s3-key notification.zip