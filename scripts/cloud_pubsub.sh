#!/usr/bin/env bash
# One-time GCP Pub/Sub setup/teardown for cloud mode (MESSAGING_BACKEND=cloud).
# Mirrors docs/RUNBOOK.md's "One-time GCP setup" + Pub/Sub teardown blocks --
# run this instead of retyping those commands by hand.
#
# Usage: scripts/cloud_pubsub.sh up|down
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

TOPIC="recentchange-raw"
SUBSCRIPTION="${TOPIC}-sub"

if [ -z "${PUBSUB_PROJECT_ID:-}" ] || [ "$PUBSUB_PROJECT_ID" = "wiki-cdc-streaming-local" ]; then
  echo "PUBSUB_PROJECT_ID in .env is still the local-emulator placeholder -- set it to your real GCP project id first." >&2
  exit 1
fi

usage() {
  echo "Usage: $0 up|down" >&2
  exit 1
}

case "${1:-}" in
  up)
    gcloud config set project "$PUBSUB_PROJECT_ID"
    gcloud services enable pubsub.googleapis.com
    echo "Pub/Sub API enabled for project $PUBSUB_PROJECT_ID."
    echo "Set MESSAGING_BACKEND=cloud in .env to use it (ensure_topic creates $TOPIC/$SUBSCRIPTION on first run)."
    ;;
  down)
    gcloud pubsub subscriptions delete "$SUBSCRIPTION" --quiet || true
    gcloud pubsub topics delete "$TOPIC" --quiet || true
    echo "Pub/Sub topic/subscription removed."
    ;;
  *)
    usage
    ;;
esac
