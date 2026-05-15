#!/usr/bin/env bash
set -euo pipefail

# This script bootstraps the remote state resources (S3 and DynamoDB) required by Terraform backend.
# It should only be run ONCE per AWS account.

REGION="us-east-1"
BUCKET_NAME="iicpc-tf-state"
TABLE_NAME="iicpc-tf-locks"

echo "Creating S3 bucket $BUCKET_NAME in $REGION..."
aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"

echo "Enabling bucket versioning..."
aws s3api put-bucket-versioning --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

echo "Enabling default bucket encryption (AES256)..."
aws s3api put-bucket-encryption --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}'

echo "Creating DynamoDB table $TABLE_NAME for state locking..."
aws dynamodb create-table --table-name "$TABLE_NAME" \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION"

echo "Bootstrap complete! You can now run 'terraform init' in infra/terraform"
