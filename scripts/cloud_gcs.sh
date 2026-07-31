#!/usr/bin/env bash
# One-time GCS setup/teardown for STORAGE_BACKEND=gcs.
# Mirrors docs/RUNBOOK.md's "Storage option B: GCS" block -- run this instead
# of retyping those commands by hand. The service account key can't be
# inlined into .env like the S3 access key (GOOGLE_APPLICATION_CREDENTIALS
# is a file-path contract), so it's written to ./gcp-credentials.json
# (gitignored) and only that path is recorded in .env.
#
# Usage: scripts/cloud_gcs.sh up|down
#
# Prerequisite: gcloud already authenticated (gcloud auth application-default
# login) -- this script only creates/removes resources, it never logs in.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

ENV_FILE=".env"
ensure_env_file "$ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"

LOCATION="${GCS_LOCATION:-us-central1}"
BUCKET="${BUCKET_NAME:-wiki-cdc-streaming-raw}"
SA_NAME="wiki-cdc-streaming-run"
KEY_FILE="./gcp-credentials.json"

if [ -z "${PUBSUB_PROJECT_ID:-}" ] || [ "$PUBSUB_PROJECT_ID" = "wiki-cdc-streaming-local" ]; then
  echo "PUBSUB_PROJECT_ID in .env is still the local-emulator placeholder -- set it to your real GCP project id first." >&2
  exit 1
fi
SA_EMAIL="${SA_NAME}@${PUBSUB_PROJECT_ID}.iam.gserviceaccount.com"

usage() {
  echo "Usage: $0 up|down" >&2
  exit 1
}

up() {
  gcloud services enable storage.googleapis.com

  if gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
    echo "Bucket gs://${BUCKET} already exists, skipping creation."
  else
    gsutil mb -l "$LOCATION" "gs://${BUCKET}"
    echo "Created bucket gs://${BUCKET} in $LOCATION."
  fi

  if gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
    echo "Service account $SA_EMAIL already exists, skipping creation."
  else
    gcloud iam service-accounts create "$SA_NAME"
    echo "Created service account $SA_EMAIL."
  fi

  gcloud projects add-iam-policy-binding "$PUBSUB_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" --role="roles/pubsub.editor" >/dev/null
  gcloud projects add-iam-policy-binding "$PUBSUB_PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" --role="roles/storage.objectAdmin" >/dev/null

  if [ -f "$KEY_FILE" ]; then
    echo "$KEY_FILE already exists -- leaving it as is (GCP doesn't re-expose an existing key's contents). Delete it and re-run for a fresh one."
  else
    gcloud iam service-accounts keys create "$KEY_FILE" --iam-account="$SA_EMAIL"
  fi

  set_env_var STORAGE_BACKEND gcs "$ENV_FILE"
  set_env_var BUCKET_NAME "$BUCKET" "$ENV_FILE"
  set_env_var GOOGLE_APPLICATION_CREDENTIALS "$KEY_FILE" "$ENV_FILE"
  echo "Wrote GCS config into $ENV_FILE (STORAGE_BACKEND=gcs)."
}

down() {
  gsutil rm -r "gs://${BUCKET}" || true
  gcloud iam service-accounts delete "$SA_EMAIL" --quiet || true
  rm -f "$KEY_FILE"

  set_env_var STORAGE_BACKEND minio "$ENV_FILE"
  uncomment_env_var S3_ENDPOINT_URL "$ENV_FILE"
  echo "GCS bucket/service account/key removed; .env reverted to STORAGE_BACKEND=minio."
}

case "${1:-}" in
up) up ;;
down) down ;;
*) usage ;;
esac
