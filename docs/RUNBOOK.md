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
# for those. One edit is required: set WIKI_STREAM_CONTACT to your own
# email/URL — Wikimedia's EventStreams endpoint rejects requests with no
# contact info in the User-Agent (403). There is no code-level default,
# by design (it's per-deployer, not something to hardcode or commit).

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

> **Cost warning:** real Pub/Sub, S3, and GCS all stay within their respective Always Free / free-tier allowances at this project's lab scale (Section 6 of `SPEC-agnostic-architecture.md`), but they are real cloud resources tied to your GCP/AWS account and billing — unlike the local stack, they are not torn down by `docker compose down`. Delete them explicitly (commands at the end of each block) when you're done experimenting with the cloud path. AWS's Free Tier historically applied only to the first 12 months on new accounts for some services — double-check your account's current Free Tier status before assuming $0 on S3.

Switching backends is configuration-only (P2) — no change to `src/producer/`, `src/consumer/`, or `src/shared/`. What changes beyond `.env` is one-time cloud-side setup that the application deliberately does not do for you. Messaging and storage are independent choices — pick a storage option (S3 or GCS) regardless of whether Pub/Sub is emulated or real.

- **Pub/Sub:** topic/subscription creation *is* automatic in cloud mode too (`ensure_topic` calls the real Pub/Sub API the same way it calls the emulator) — you only need a GCP project with the Pub/Sub API enabled and a credential.
- **S3 bucket:** creation is deliberately **not** automatic when `S3_ENDPOINT_URL` is unset (`src/handlers/storage/s3_compatible_storage_handler.py`) — per P3 (no cloud resource turned on by default), you create it explicitly, once, below. **Required if you plan to run Phase 2+ on Databricks Free Edition**, whose External Volumes only support S3-backed storage, not GCS.
- **GCS bucket:** creation is deliberately **not** automatic (`src/handlers/storage/gcs_storage_handler.py`) — same P3 reasoning, same explicit-creation requirement.

```bash
# One-time GCP setup (Pub/Sub — needed regardless of which storage option you pick)
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
gcloud services enable pubsub.googleapis.com
```

#### Storage option A: S3 (required for Databricks Free Edition)

> **Region:** create the bucket in **`us-east-2`**, matching this project's Databricks Free Edition workspace region — an External Volume registered against a bucket in a different region adds cross-region latency/transfer friction for no benefit here. Substitute your own workspace's region if it differs.

```bash
# One-time AWS setup
aws configure   # or export AWS_PROFILE=<your-profile>, if not already configured

# Create the bucket (us-east-1 is the one region that would omit --create-bucket-configuration)
aws s3api create-bucket --bucket <your-bucket-name> --region us-east-2 \
  --create-bucket-configuration LocationConstraint=us-east-2

# Dedicated IAM user + access key, scoped to just this bucket (least privilege)
aws iam create-user --user-name wiki-cdc-streaming-run
aws iam put-user-policy --user-name wiki-cdc-streaming-run --policy-name wiki-cdc-streaming-s3-access \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<your-bucket-name>", "arn:aws:s3:::<your-bucket-name>/*"]
    }]
  }'
aws iam create-access-key --user-name wiki-cdc-streaming-run
# copy AccessKeyId/SecretAccessKey from the output into .env below
```

Update `.env` (keep the rest of the file untouched — critically, `S3_ENDPOINT_URL` must be **unset/removed**, not just left at its local value, or `S3CompatibleStorageHandler` will still point at MinIO):

```bash
MESSAGING_BACKEND=cloud
PUBSUB_PROJECT_ID=<your-gcp-project-id>
STORAGE_BACKEND=s3
BUCKET_NAME=<your-bucket-name>
AWS_ACCESS_KEY_ID=<the-access-key-id-from-above>
AWS_SECRET_ACCESS_KEY=<the-secret-access-key-from-above>
AWS_REGION=us-east-2
# S3_ENDPOINT_URL removed entirely
```

```bash
# Teardown — avoid any lingering cost/resource
aws s3 rm s3://<your-bucket-name> --recursive
aws s3api delete-bucket --bucket <your-bucket-name> --region us-east-2
aws iam delete-access-key --user-name wiki-cdc-streaming-run --access-key-id <the-access-key-id>
aws iam delete-user-policy --user-name wiki-cdc-streaming-run --policy-name wiki-cdc-streaming-s3-access
aws iam delete-user --user-name wiki-cdc-streaming-run
```

#### Storage option B: GCS

```bash
# One-time GCP setup (in addition to the Pub/Sub setup above)
gcloud services enable storage.googleapis.com

# Create the GCS bucket (Always Free tier: 5GB-month, single region)
gsutil mb -l us-central1 gs://<your-bucket-name>

# Service account credential for GOOGLE_APPLICATION_CREDENTIALS
gcloud iam service-accounts create wiki-cdc-streaming-run
gcloud projects add-iam-policy-binding <your-gcp-project-id> \
  --member="serviceAccount:wiki-cdc-streaming-run@<your-gcp-project-id>.iam.gserviceaccount.com" \
  --role="roles/pubsub.editor"
gcloud projects add-iam-policy-binding <your-gcp-project-id> \
  --member="serviceAccount:wiki-cdc-streaming-run@<your-gcp-project-id>.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
gcloud iam service-accounts keys create ./gcp-credentials.json \
  --iam-account=wiki-cdc-streaming-run@<your-gcp-project-id>.iam.gserviceaccount.com
```

Update `.env` (keep the rest of the file untouched):

```bash
MESSAGING_BACKEND=cloud
PUBSUB_PROJECT_ID=<your-gcp-project-id>
STORAGE_BACKEND=gcs
BUCKET_NAME=<your-bucket-name>
GOOGLE_APPLICATION_CREDENTIALS=./gcp-credentials.json
```

```bash
# Teardown — avoid any lingering cost/resource
gsutil rm -r gs://<your-bucket-name>
gcloud iam service-accounts delete wiki-cdc-streaming-run@<your-gcp-project-id>.iam.gserviceaccount.com
rm ./gcp-credentials.json
```

#### Pub/Sub teardown (either storage option)

```bash
gcloud pubsub subscriptions delete recentchange-raw-sub
gcloud pubsub topics delete recentchange-raw
```

Run producer/consumer exactly as in the local run section above — same commands, no code change, regardless of which storage option you picked.

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
