#!/usr/bin/env bash
# One-time AWS S3 setup/teardown for STORAGE_BACKEND=s3.
# Mirrors docs/RUNBOOK.md's "Storage option A: S3" block -- run this instead
# of retyping those commands by hand. Generated credentials are captured
# into shell variables and written straight into .env; nothing lingers in a
# separate file, and .env.example (the committed template) is never touched.
#
# Usage: scripts/cloud_s3.sh up|down
#
# Prerequisite: aws CLI already authenticated (aws configure / AWS_PROFILE)
# -- this script only creates/removes resources, it never logs in.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

ENV_FILE=".env"
ensure_env_file "$ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"

REGION="${AWS_REGION:-us-east-2}"
BUCKET="${BUCKET_NAME:-wiki-cdc-streaming-raw}"
IAM_USER="wiki-cdc-streaming-run"
POLICY_NAME="wiki-cdc-streaming-s3-access"

usage() {
  echo "Usage: $0 up|down" >&2
  exit 1
}

up() {
  if aws s3api head-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null; then
    echo "Bucket $BUCKET already exists, skipping creation."
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
    echo "Created bucket $BUCKET in $REGION."
  fi

  if aws iam get-user --user-name "$IAM_USER" >/dev/null 2>&1; then
    echo "IAM user $IAM_USER already exists, skipping creation."
  else
    aws iam create-user --user-name "$IAM_USER"
    echo "Created IAM user $IAM_USER."
  fi

  aws iam put-user-policy --user-name "$IAM_USER" --policy-name "$POLICY_NAME" \
    --policy-document "{
      \"Version\": \"2012-10-17\",
      \"Statement\": [{
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::${BUCKET}\", \"arn:aws:s3:::${BUCKET}/*\"]
      }]
    }"

  local existing_keys
  existing_keys="$(aws iam list-access-keys --user-name "$IAM_USER" \
    --query 'AccessKeyMetadata[].AccessKeyId' --output text)"
  if [ -n "$existing_keys" ]; then
    echo "IAM user $IAM_USER already has an access key ($existing_keys) -- AWS never re-exposes an existing secret, so .env is left untouched. Run '$0 down' then '$0 up' again for a fresh key if needed."
  else
    local access_key_id secret_access_key
    read -r access_key_id secret_access_key <<<"$(
      aws iam create-access-key --user-name "$IAM_USER" \
        --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text
    )"

    set_env_var STORAGE_BACKEND s3 "$ENV_FILE"
    set_env_var BUCKET_NAME "$BUCKET" "$ENV_FILE"
    set_env_var AWS_ACCESS_KEY_ID "$access_key_id" "$ENV_FILE"
    set_env_var AWS_SECRET_ACCESS_KEY "$secret_access_key" "$ENV_FILE"
    set_env_var AWS_REGION "$REGION" "$ENV_FILE"
    comment_env_var S3_ENDPOINT_URL "$ENV_FILE"
    unset access_key_id secret_access_key

    echo "Wrote the new access key into $ENV_FILE (STORAGE_BACKEND=s3, S3_ENDPOINT_URL disabled)."
  fi
}

down() {
  aws s3 rm "s3://${BUCKET}" --recursive --region "$REGION" || true
  aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" || true

  local key_id
  for key_id in $(aws iam list-access-keys --user-name "$IAM_USER" \
    --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
    aws iam delete-access-key --user-name "$IAM_USER" --access-key-id "$key_id"
  done
  aws iam delete-user-policy --user-name "$IAM_USER" --policy-name "$POLICY_NAME" 2>/dev/null || true
  aws iam delete-user --user-name "$IAM_USER" 2>/dev/null || true

  set_env_var STORAGE_BACKEND minio "$ENV_FILE"
  uncomment_env_var S3_ENDPOINT_URL "$ENV_FILE"
  echo "S3 bucket/IAM user/access key removed; .env reverted to STORAGE_BACKEND=minio."
}

case "${1:-}" in
up) up ;;
down) down ;;
*) usage ;;
esac
