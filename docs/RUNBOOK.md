# RUNBOOK — Execution Guide (Local + Cloud)

Strictly detailed, step-by-step execution for each phase — local (default, $0) and cloud (optional). Every cloud step carries an explicit cost warning. This document doesn't justify design decisions (see `docs/trade-offs.md`) — it only tells you what to run.

---

## Phase 1 — Ingestion

### Local setup

```bash
# 1. Dependencies (uv, never pip/venv/conda directly)
uv sync

# 2. Environment variables
cp .env.example .env
# Defaults already match the local stack (MESSAGING_BACKEND=emulator,
# STORAGE_BACKEND=minio, localhost:8085/localhost:9000) — no edits needed
# for a pure local run.

# 3. Bring up the local stack (Pub/Sub Emulator + MinIO)
docker compose -f local-stack/docker-compose.yml up -d
```

Topic/subscription creation (Pub/Sub Emulator) and bucket creation (MinIO) are both handled automatically at startup by the application code (`ensure_topic`, `bucket_exists`/`make_bucket`) — no manual provisioning step locally.

### Local run

```bash
# Terminal 1 — consumer (reads messaging, writes raw Parquet to MinIO)
uv run python -m src.consumer.main

# Terminal 2 — producer (consumes the Wikipedia recentchange SSE, publishes to messaging)
uv run python -m src.producer.main
```

- MinIO console: [localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`) — verify `raw/dt=.../hour=.../part-*.parquet` files appear as the producer runs.
- Stop each process with `Ctrl+C` — both producer and consumer log a clean shutdown message and, for the consumer, perform a final buffer flush before exiting.

```bash
# Tear down (no leftover process/cost)
docker compose -f local-stack/docker-compose.yml down -v
```

### Cloud (optional)

> **Cost warning:** real Pub/Sub and GCS both stay within the GCP Always Free tier at this project's lab scale, but they are real cloud resources tied to your GCP account/billing — unlike the local stack, they are not torn down by `docker compose down`. Delete them explicitly (commands at the end of this section) when you're done experimenting with the cloud path.

Switching backends is configuration-only (P2) — no change to `src/producer/`, `src/consumer/`, or `src/shared/`. What changes beyond `.env` is one-time cloud-side setup that the application deliberately does not do for you:

- **Pub/Sub:** topic/subscription creation *is* automatic in cloud mode too (`ensure_topic` calls the real Pub/Sub API the same way it calls the emulator) — you only need a GCP project with the Pub/Sub API enabled and a credential.
- **GCS bucket:** creation is deliberately **not** automatic (`src/handlers/storage/gcs_storage_handler.py`) — per P3 (no cloud resource turned on by default), you create it explicitly, once, below.

```bash
# One-time GCP setup
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
gcloud services enable pubsub.googleapis.com storage.googleapis.com

# Create the GCS bucket (Always Free tier: 5GB-month, single region)
gsutil mb -l us-central1 gs://<your-bucket-name>

# Service account credential for GOOGLE_APPLICATION_CREDENTIALS
gcloud iam service-accounts create cdcstream-local-run
gcloud projects add-iam-policy-binding <your-gcp-project-id> \
  --member="serviceAccount:cdcstream-local-run@<your-gcp-project-id>.iam.gserviceaccount.com" \
  --role="roles/pubsub.editor"
gcloud projects add-iam-policy-binding <your-gcp-project-id> \
  --member="serviceAccount:cdcstream-local-run@<your-gcp-project-id>.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
gcloud iam service-accounts keys create ./gcp-credentials.json \
  --iam-account=cdcstream-local-run@<your-gcp-project-id>.iam.gserviceaccount.com
```

Update `.env` (keep the rest of the file untouched):

```bash
MESSAGING_BACKEND=cloud
PUBSUB_PROJECT_ID=<your-gcp-project-id>
STORAGE_BACKEND=gcs
BUCKET_NAME=<your-bucket-name>
GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json
```

Run producer/consumer exactly as in the local run section above — same commands, no code change.

```bash
# Teardown — avoid any lingering cost/resource
gsutil rm -r gs://<your-bucket-name>
gcloud pubsub subscriptions delete recentchange-raw-sub
gcloud pubsub topics delete recentchange-raw
gcloud iam service-accounts delete cdcstream-local-run@<your-gcp-project-id>.iam.gserviceaccount.com
rm ./gcp-credentials.json
```

---

## Phase 2 — Bronze (consolidation via Spark)

### Local setup

*To be filled in during Phase 2 implementation.*

### Local run

*To be filled in during Phase 2 implementation.*

### Cloud (optional)

*To be filled in during Phase 2 implementation (Databricks Free Edition setup, external GCS credential from serverless compute).*

---

## Phase 2.5 — CI/CD

### Local setup

*To be filled in during Phase 2.5 implementation.*

### Local run

*To be filled in during Phase 2.5 implementation.*

### Cloud (optional)

*To be filled in during Phase 2.5 implementation (manual `workflow_dispatch` trigger for Databricks Jobs API deploy).*

---

## Phase 3 — Silver (Data Quality + Change Data Feed)

### Local setup

*To be filled in during Phase 3 implementation.*

### Local run

*To be filled in during Phase 3 implementation.*

### Cloud (optional)

*To be filled in during Phase 3 implementation.*

---

## Phase 4 — Gold (Dimensional Modeling + Streamlit Dashboard)

### Local setup

*To be filled in during Phase 4 implementation.*

### Local run

*To be filled in during Phase 4 implementation.*

### Cloud (optional)

*To be filled in during Phase 4 implementation (Streamlit Community Cloud deploy, if used).*
