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

> **Shortcut:** every command block below has a matching `scripts/cloud_{pubsub,s3,gcs}.sh up|down` that runs it for you — idempotent (safe to re-run), and the storage scripts write the resulting credentials straight into `.env` (auto-created from `.env.example` on first run) instead of you copy-pasting them. They assume `aws`/`gcloud` are already authenticated locally. The manual commands are kept below too, since that's what the scripts actually run — useful if you'd rather type them yourself or understand exactly what's happening before running something that touches real cloud billing.

- **Pub/Sub:** topic/subscription creation *is* automatic in cloud mode too (`ensure_topic` calls the real Pub/Sub API the same way it calls the emulator) — you only need a GCP project with the Pub/Sub API enabled and a credential.
- **S3 bucket:** creation is deliberately **not** automatic when `S3_ENDPOINT_URL` is unset (`src/handlers/storage/s3_compatible_storage_handler.py`) — per P3 (no cloud resource turned on by default), you create it explicitly, once, below. **Required if you plan to run Phase 2+ on Databricks Free Edition**, whose External Volumes only support S3-backed storage, not GCS.
- **GCS bucket:** creation is deliberately **not** automatic (`src/handlers/storage/gcs_storage_handler.py`) — same P3 reasoning, same explicit-creation requirement.

```bash
# One-time GCP setup (Pub/Sub — needed regardless of which storage option you pick)
gcloud auth application-default login
gcloud config set project <your-gcp-project-id>
gcloud services enable pubsub.googleapis.com
```

Shortcut: `scripts/cloud_pubsub.sh up` (after setting `PUBSUB_PROJECT_ID` in `.env` to your real GCP project id — it refuses to run against the local-emulator placeholder).

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

Shortcut: `scripts/cloud_s3.sh up` runs everything above **and** writes the resulting `AccessKeyId`/`SecretAccessKey` into `.env` for you (plus `STORAGE_BACKEND=s3`, `BUCKET_NAME`, `AWS_REGION`, and disabling `S3_ENDPOINT_URL`) — skip straight to the local run section afterward.

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

Shortcut: `scripts/cloud_s3.sh down` (also reverts `.env` to `STORAGE_BACKEND=minio`).

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

Shortcut: `scripts/cloud_gcs.sh up` runs everything above and writes `STORAGE_BACKEND=gcs`, `BUCKET_NAME`, and `GOOGLE_APPLICATION_CREDENTIALS` into `.env` for you — skip straight to the local run section afterward.

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

Shortcut: `scripts/cloud_gcs.sh down` (also reverts `.env` to `STORAGE_BACKEND=minio`).

#### Pub/Sub teardown (either storage option)

```bash
gcloud pubsub subscriptions delete recentchange-raw-sub
gcloud pubsub topics delete recentchange-raw
```

Shortcut: `scripts/cloud_pubsub.sh down`.

Run producer/consumer exactly as in the local run section above — same commands, no code change, regardless of which storage option you picked.

---

## Phase 2 — Bronze (consolidation via Spark)

### Local setup

```bash
# 1. Dependencies (uv add "pyspark[pipelines]" delta-spark duckdb already
#    recorded in pyproject.toml/uv.lock -- uv sync installs them)
uv sync

# 2. Prerequisite: Spark 4.1+/SDP needs JDK 17+ -- stricter than Phase 1's
#    plain PySpark requirement
java -version

# 3. Bring up the local stack if it isn't already running (Phase 1's MinIO)
docker compose -f local-stack/docker-compose.yml up -d

# 4. Generate the local, machine-specific Spark configuration from .env --
#    a persistent local Hive metastore + the Hadoop S3A/GCS connector jars,
#    neither of which spark-pipeline.yml's own configuration: block can set
#    (see docs/SPEC-phase2-bronze.md Section 4.3 for why). Re-run this any
#    time .env changes (STORAGE_BACKEND, credentials, bucket name).
uv run python -m scripts.render_local_spark_config

# 5. Point Spark at that generated config for every command below (every
#    new shell)
export SPARK_CONF_DIR="$(pwd)/local-stack/.spark-conf"
```

Neither `pipelines/spark-pipeline.yml` nor `local-stack/.spark-conf/` is committed — both are generated by step 4 from `.env` and gitignored, mirroring the `.env.example` → `.env` convention. `pipelines/spark-pipeline.yml.example` is the committed template.

### Local run

```bash
# Required before the very first dry-run/run: bronze_dim_wiki_reference is
# a *batch* read (unlike bronze_recentchange's streaming read, which
# tolerates starting with zero files) -- it fails with [PATH_NOT_FOUND] if
# dim_wiki_reference/ doesn't have at least one snapshot file yet. A real
# call to the public Wikimedia sitematrix API -- no credential needed.
uv run python -m scripts.fetch_wiki_sitematrix

# Validate the pipeline definition without touching data
uv run spark-pipelines dry-run --spec pipelines/spark-pipeline.yml

# Run it — processes new raw files since the last run, then stops
uv run spark-pipelines run --spec pipelines/spark-pipeline.yml

# Pull a fresh wiki-metadata snapshot whenever you want
# bronze_dim_wiki_reference to reflect current data
uv run python -m scripts.fetch_wiki_sitematrix

# Inspect the results locally (DuckDB's delta_scan(), no SparkSession needed)
uv run python -m scripts.inspect_bronze
```

Things worth knowing before you run these:

- **Fetch a wiki-reference snapshot before the first dry-run/run, not after** — see the comment above. This trips up a first-time run since `bronze_recentchange` alone doesn't need it.
- **Always run `spark-pipelines` commands from the repo root.** The embedded Derby metastore's own location (`metastore_db/`, `derby.log`) can't be redirected via `spark-defaults.conf` — it always lands relative to wherever the command is invoked from (both are gitignored at the repo root). `spark.sql.warehouse.dir` (the actual table data) *is* redirected correctly, to `local-stack/.spark-conf/warehouse/`.
- **Two variants of `bronze_dim_wiki_reference` run every time**, side by side, for comparison (`bronze_dim_wiki_reference_py` / `bronze_dim_wiki_reference_sql` — convention 9.5.1). Once you've picked one, rename its table back to `bronze_dim_wiki_reference` and delete the other file (`pipelines/bronze/dim_wiki_reference.py` or `.sql`).
- **First run downloads jars** (Hadoop-AWS, Delta, or the GCS connector, depending on `STORAGE_BACKEND`) via Maven — a one-time cost per machine, cached afterward in `~/.ivy2.5.2/`.
- **Tear down when done:** `docker compose -f local-stack/docker-compose.yml down -v` (no leftover process/cost — MinIO only; the generated Spark config and `metastore_db`/`spark-warehouse` are just local files, safe to leave or `rm -rf` freely, they'll regenerate).

### Cloud (optional)

> **Cost warning:** this uses Databricks Free Edition serverless compute, which stays within its fair-use quota at this project's scale, but is still a real cloud resource tied to your Databricks account. Databricks Free Edition's External Locations only support S3-backed storage (not GCS, not MinIO) — see Phase 1's Cloud section (`STORAGE_BACKEND=s3`) if you haven't set that up yet.
>
> **Steps 1–2 and 5 are exact UI flows I haven't verified against a live Databricks workspace** — expect to troubleshoot the precise menu labels yourself; report back what you actually see and this section gets corrected. **Step 3 (no AWS credentials needed) is verified directly from the code**, not a guess — see below.

Switching compute to Databricks doesn't touch `pipelines/bronze/*.py`/`*.sql` — same source files, same `STORAGE_BACKEND`-driven config (P2/P6). What's genuinely different is one-time cloud-side setup:

1. **Seed real S3 first.** Set `STORAGE_BACKEND=s3` in `.env` (Phase 1's Cloud section covers the one-time AWS bucket/IAM setup), run producer/consumer against it for a bit to get raw data in, then `uv run python -m scripts.fetch_wiki_sitematrix` to seed `dim_wiki_reference/`. Databricks Free Edition can't reach a local MinIO instance.
2. **IAM role + Unity Catalog Storage Credential + External Location**, registered against your S3 bucket (Catalog → External Data → Credentials, then External Locations) — the standard Unity-Catalog-to-S3 setup, granting Databricks compute access to that bucket path.
3. **Git folder**, not a manual sync: Workspace → Create → Git folder → this repo's URL → check out the relevant branch. Then, in that Git folder, create a `.env` file (Workspace UI → right-click → Create → File) at the repo root containing only:

   ```bash
   STORAGE_BACKEND=s3
   BUCKET_NAME=<your-real-bucket-name>
   ```

   No AWS keys needed, and this isn't a shortcut/placeholder — it's the actual requirement. `S3CompatibleStorageHandler`/`GcsStorageHandler` build their real client (and read `AWS_ACCESS_KEY_ID`/`SECRET_ACCESS_KEY`/`REGION`) lazily, only on `write()`/`read()` — `pipelines/bronze/*.py` call only `resolve_uri()`, which needs just the bucket name. The actual S3 read happens through Spark's own Hadoop S3A connector, using whatever credential Unity Catalog vends via the External Location from step 2 — a completely separate mechanism from these Python-level env vars, and one that never needs an explicit AWS secret sitting in the workspace. Verified directly: `resolve_uri()` returns the correct URI with zero AWS env vars set at all (`tests/test_s3_compatible_storage_handler.py::test_resolve_uri_needs_no_aws_credentials`).
4. Create a Lakeflow Declarative Pipeline (Jobs & Pipelines → Create → ETL Pipeline) with source code pointing at the Git folder's `pipelines/bronze/` path, destination a Unity Catalog catalog/schema of your choice, compute Serverless. In its Configuration section add `spark.wikicdc.raw_dim_wiki_reference_path` = `s3a://<your-bucket>/dim_wiki_reference` (only `dim_wiki_reference.sql` needs this — the `.py` variants resolve their own path).
5. Run the pipeline from the Databricks UI — should show `bronze_recentchange`, `bronze_dim_wiki_reference_py`, `bronze_dim_wiki_reference_sql` as three graph nodes. This is also the one environment where the SDP features this phase deliberately avoided locally — expectations, `read_files`, `create_auto_cdc_flow` (`docs/SPEC-phase2-bronze.md` Section 4.1/4.2/5) — are genuinely available, should a future phase revisit using them.
6. Optional: wrap the pipeline in a schedule via Workflows → Jobs → Create Job → add a task of type Pipeline → select the pipeline from step 4 → set a schedule.

Teardown: delete the Lakeflow pipeline, Job (if created), Git folder, and the External Location/Storage Credential from the Databricks UI when done; `scripts/cloud_s3.sh down` (Phase 1) tears down the underlying S3 bucket/IAM user.

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
