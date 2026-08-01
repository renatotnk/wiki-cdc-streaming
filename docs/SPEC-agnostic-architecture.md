# SPEC — Agnostic Pipeline Architecture (Ingestion → Medallion Lakehouse)

**Status:** Approved for implementation
**Scope of this document:** macro architecture, interface contracts between pluggable components, and an overview of each phase. Each phase has its own SPEC (`SPEC-phase{N}-*.md`) that references this document by contract — it does not duplicate decisions already made here.

---

## 1. Objective

Build a complete data pipeline — from ingesting a real-time change feed to a consumable medallion Lakehouse (bronze/silver/gold) — using a single real data source, with **every piece of infrastructure swappable between local and cloud execution via configuration, never via logic changes**.

The pipeline serves two purposes simultaneously:
1. A public technical portfolio (GitHub), reproducible by third parties without access to the author's cloud account.
2. A study vehicle for: event ingestion, messaging, Delta Lake, Spark, Databricks, CI/CD, applied FinOps, dimensional modeling, and data visualization.

---

## 2. Architectural principles (non-negotiable)

| # | Principle | Practical implication |
|---|---|---|
| P0 | **Avoid over-engineering** | The principle that governs all the others — when in doubt, it wins. See `docs/ENGINEERING-PRINCIPLES.md` for warning signs and the practical test ("does this solve a requirement that exists now, or one I imagine will exist?"). |
| P1 | **Local-first, cloud optional** | Every component must run via `docker-compose` without any cloud credential. Cloud is an additional mode, never a requirement. |
| P2 | **Swap via configuration, not code** | No `if cloud: ... else: ...` branch in business logic. Backend swap = environment variable + credential. See contracts in Section 4. |
| P3 | **Minimum cost by default** | No cloud resource stays on by default. Any cloud resource created must have a shutdown command documented in the same SPEC that creates it. |
| P4 | **Reconciliation invariant** | `bronze_count = silver_ok_count + silver_rejected_count`. An invalid row is never silently dropped — it goes to a rejects table. |
| P5 | **Structured logging** | JSON via stdout only. Never writes to a log file (breaks portability across environments). |
| P6 | **Spark logic portability** | The same transformation code (bronze→silver→gold) runs on `local[*]` and on Databricks. Changes are only allowed at the session/cluster configuration layer. |
| P7 | **One data source, one domain** | Wikipedia EventStreams (`recentchange`) is the only real data source of the main pipeline. There is no relational database in the main pipeline (see Section 8 — appendix). |
| P8 | **Versioned data contracts between layers** | Every boundary between layers (raw→bronze, bronze→silver, silver→gold) has a versioned contract file in `contracts/` declaring the expected schema and nullability. A contracted-field violation fails loud (fail-fast); a new, non-contracted field is tolerated via an escape hatch (`_extra_fields`), never dropped and never breaks the pipeline. See Section 4.4. |

---

## 3. Data source

**Source:** Wikipedia EventStreams — public stream via Server-Sent Events (SSE).
**Endpoint:** `https://stream.wikimedia.org/v2/stream/recentchange`
**Nature:** a change feed already published by the source (functionally analogous to a CDC feed, but without transaction-log capture — the source already exposes the change event directly).

### Relevant event fields (subset used in the pipeline)

| Field | Type | Description |
|---|---|---|
| `id` | long | Sequential event ID (used for dedup/idempotency) |
| `type` | string | `edit`, `new`, `log`, `categorize` |
| `title` | string | Title of the edited page |
| `user` | string | User who made the change |
| `bot` | boolean | Whether the change was made by a bot |
| `wiki` | string | Wiki domain (e.g., `enwiki`, `ptwiki`) |
| `timestamp` | long (epoch) | Event moment (event time) |
| `server_url` | string | Origin server URL |
| `meta.dt` | ISO8601 timestamp | Event publication moment (used for watermarking) |
| `length.old` / `length.new` | int | Page size before/after (used to detect large edits/drift) |

Full schema: [mediawiki/recentchange JSON schema](https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/recentchange).

---

## 4. Interface contracts (pluggable components)

Each component below is implemented as an abstract interface + at least two concrete implementations (local and cloud). Claude Code must implement against the interface, never against the concrete implementation, in business logic layers.

### 4.1 Messaging interface

```python
class MessagingBackend(Protocol):
    def publish(self, topic: str, message: dict) -> None: ...
    def subscribe(self, subscription: str, callback: Callable[[dict], None]) -> None: ...
    def ensure_topic(self, topic: str) -> None: ...
```

| Implementation | Real backend | When to use |
|---|---|---|
| `PubSubEmulatorBackend` | Pub/Sub Emulator (Docker, `google-cloud-pubsub` pointed at the emulator) | Default, local, $0 |
| `PubSubCloudBackend` | Real Pub/Sub (GCP) | Optional, minimal cost (free tier covers lab-scale volume) |

**Config:** `MESSAGING_BACKEND=emulator|cloud`, `PUBSUB_PROJECT_ID`, `PUBSUB_EMULATOR_HOST` (only when `emulator`).

### 4.2 Storage interface

```python
class StorageBackend(Protocol):
    def write(self, df, path: str, format: str = "delta") -> None: ...
    def read(self, path: str, format: str = "delta"): ...
    def resolve_uri(self, logical_path: str) -> str: ...
```

| Implementation | Real backend | Resulting URI |
|---|---|---|
| `S3CompatibleStorageHandler` | MinIO (Docker, local) **or** real AWS S3 — same class, see below | `s3a://<bucket>/<logical_path>` |
| `GcsStorageBackend` | Google Cloud Storage | `gs://<bucket>/<logical_path>` |

**Why one class for both MinIO and S3:** MinIO speaks the S3 API — the only structural difference is that MinIO needs a custom endpoint (`fs.s3a.endpoint`) and real S3 doesn't. Rather than duplicating near-identical code across two handler classes, `S3CompatibleStorageHandler` takes an optional endpoint: set → MinIO (or any other S3-compatible store); unset → real AWS S3. This is DRY applied to the handler layer itself, not just to business logic.

**Config:** `STORAGE_BACKEND=minio|s3|gcs`, `BUCKET_NAME`. `minio` and `s3` both resolve to `S3CompatibleStorageHandler`, differing only in whether `S3_ENDPOINT_URL` is set:

| Variable | `minio` | `s3` | `gcs` |
|---|---|---|---|
| `S3_ENDPOINT_URL` | set (e.g., `http://localhost:9000`) | unset | — |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | MinIO credentials | real AWS credentials | — |
| `AWS_REGION` | ignored | required | — |
| `GOOGLE_APPLICATION_CREDENTIALS` | — | — | required |

Swapping backend **does not change any read/write call in the transformation code** — only the value resolved by `resolve_uri()` and which credentials are picked up.

> **Databricks Free Edition constraint:** external connections and External Volumes on Free Edition currently only support S3-backed storage — not GCS. This means the practical cloud pairing for this project is **Databricks Free Edition + S3**, not Databricks + GCS. GCS remains part of the interface (useful if compute ever moves to a non-Databricks Spark environment on GCP), but isn't usable together with Databricks Free Edition specifically.

### 4.3 Compute interface (Spark Declarative Pipelines)

**Phases 2, 3, and 4 are implemented as Spark Declarative Pipelines (SDP)**, not hand-rolled Structured Streaming jobs. SDP is a genuinely open-source Apache Spark capability (available from Spark 4.1 via the `pyspark.pipelines` module and the `spark-pipelines` CLI) — Databricks' Lakeflow Declarative Pipelines is the same framework extended to run on the managed Databricks Runtime. This is what makes P6 (portability) hold for a declarative pipeline: the same pipeline definition runs via `spark-pipelines run` locally and as a Lakeflow pipeline on Databricks, with no code fork between the two.

| Implementation | Real backend | When to use |
|---|---|---|
| Local pipeline run | `spark-pipelines run` against local Spark 4.1+ (`local[*]`) | Default, development and CI |
| Lakeflow Declarative Pipelines | Same pipeline spec, run on Databricks (Free Edition or cluster) | Portability validation, occasional use |

**Config:** the pipeline spec (`spark-pipeline.yml`) and its `configuration:` block hold any environment-varying value (storage path, checkpoint location) — pipeline source files (Python or SQL) never branch on environment. Each phase provides **both a Python and a SQL variant** of its pipeline definitions, functionally equivalent — pick whichever fits the portfolio narrative better; this SPEC doesn't mandate one over the other.

> **Prerequisite:** Spark 4.1+ and Java 17+ (not the JDK 8/11 range mentioned for earlier PySpark versions). Install via `uv add "pyspark[pipelines]" delta-spark`.

> **Compute and storage are orthogonal — Databricks is not a storage location.** Regardless of where the pipeline runs, data always lives wherever `StorageBackend` resolves to (MinIO locally, S3 in the cloud via Databricks External Volumes) — the pipeline spec's `configuration` block just points at that path. Unity Catalog External Volumes, when used on the Databricks side, are a governance/access layer registered on top of that S3 bucket path, not an alternative place where the bytes are stored. This applies identically to Phases 2, 3, and 4.

### 4.4 Data Contracts (schema boundaries between layers)

Unlike the interface contracts above (which deal with *pluggable backends*), a **data contract** deals with the *shape of the data* at a boundary between layers — it's what guarantees that whoever consumes bronze knows exactly what to expect from it, in an auditable, versioned way, instead of inferring it from code.

- Each relevant boundary (`raw→bronze`, `bronze→silver`, `silver→gold`) has a YAML file in `contracts/`, e.g., `contracts/bronze_recentchange.contract.yaml`, containing: contract version, required fields with type/nullability, and expected enum values (e.g., `type ∈ {edit, new, log, categorize}`). The `silver→gold` boundary (`gold_star_schema.contract.yaml`) covers the fact table and the incremental dimensions — static/seed dimensions (`dim_date`, `dim_change_type`) don't need their own contract since they're trivial and not derived from streaming; `dim_wiki_reference` (Section 7, Phase 2) has its own contract since it comes from a genuinely external source.
- **Two kinds of deviation, two different treatments:**
  - *New, non-contracted field* (genuine schema drift) → tolerated, captured in `_extra_fields`, logged as `INFO`. Never brings the pipeline down.
  - *Contracted field missing or changed to an incompatible type* (contract breach) → fail-fast, same P4/fail-fast principle already used in Phase 2 for structural corruption — expressed as an `@expect_or_fail`/`EXPECT ... ON VIOLATION FAIL UPDATE` pipeline expectation (Python/SQL respectively), not a custom check.
- The contract is the source of truth for the schema table documented in each phase SPEC — the prose table references the file, it doesn't duplicate the types.
- Business rules (e.g., `type` enum, value ranges) used in the Phase 3 Data Quality checks are informed by the same contract, avoiding the same constant (e.g., the list of valid `type` values) being defined in two places (DRY).

---

## 5. Repository structure

```
wiki-cdc-streaming/
├── README.md
├── .env.example                             ← template, no real values (convention 9.8)
├── .gitignore                               ← covers .env, .venv/, __pycache__/
├── pyproject.toml                           ← uv project (convention 9.7)
├── uv.lock
├── requirements.txt                         ← exported from uv.lock, committed
├── docs/
│   ├── SPEC-agnostic-architecture.md      ← this document
│   ├── SPEC-phase1-ingestion.md
│   ├── SPEC-phase2-bronze.md
│   ├── SPEC-phase2__5-cicd.md
│   ├── SPEC-phase3-silver-cdf.md
│   ├── SPEC-phase4-gold-consumption.md
│   ├── RUNBOOK.md                           ← local + cloud execution, costs, ready-to-paste cloud blocks (Section 10)
│   ├── trade-offs.md
│   ├── ENGINEERING-PRINCIPLES.md            ← project-agnostic (Section 9.6)
│   ├── implementation-workflow.md           ← repeatable per-phase process (Section 10)
│   ├── PROGRESS.md                          ← durable text checkpoint between sessions (Section 11)
│   ├── CLAUDE.md
│   └── evidence/
├── contracts/
│   ├── bronze_recentchange.contract.yaml
│   ├── silver_recentchange.contract.yaml
│   └── gold_star_schema.contract.yaml
├── local-stack/
│   └── docker-compose.yml                  ← Pub/Sub Emulator + MinIO
├── infra/terraform/                        ← optional cloud resources (Always Free where applicable)
├── src/
│   ├── interfaces/                         ← messaging.py, storage.py contracts from Section 4 (compute.py removed — SDP owns the compute layer now)
│   ├── shared/                              ← code reused across phases (DRY, convention 9.3)
│   ├── handlers/                            ← external integrations (convention 9.2)
│   ├── producer/                           ← consumes SSE, publishes (Handler)
│   └── consumer/                           ← reads messaging, writes raw (Handler)
├── pipelines/                                ← Spark Declarative Pipelines (convention 9.5)
│   ├── spark-pipeline.yml                    # one spec, all phases as one DAG (see phase SPECs for per-phase detail)
│   ├── bronze/
│   │   ├── bronze_recentchange.py            # pick one: Python variant
│   │   ├── bronze_recentchange.sql           # or: SQL variant
│   │   ├── dim_wiki_reference.py             # new: batch dimension from an external source (Section 7, Phase 2)
│   │   └── dim_wiki_reference.sql
│   ├── silver/
│   │   ├── silver_recentchange.py
│   │   └── silver_recentchange.sql
│   └── gold/
│       ├── gold_star_schema.py
│       └── gold_star_schema.sql
├── scripts/                                 ← local utilities (inspection, seed) — never a phase deliverable
│   ├── inspect_bronze.py
│   ├── inspect_silver_rejected.py
│   └── fetch_wiki_sitematrix.py              # new: pulls the external reference snapshot (Section 7, Phase 2)
├── dashboard/                                ← Streamlit, outside src/ since it's not part of the data pipeline
│   ├── app.py
│   └── data_access.py
├── tests/
│   └── test_reconciliation.py
└── .github/workflows/ci.yml
```

---

## 6. Cost and execution matrix

| Component | Local mode (default) | Cloud mode (optional) | Cloud cost |
|---|---|---|---|
| Messaging | Pub/Sub Emulator | Pub/Sub | ~$0 (within free tier) |
| Storage | MinIO | S3 (required for Databricks Free Edition External Volumes) or GCS (non-Databricks Spark on GCP) | ~$0 (S3 free tier covers lab volume; GCS Always Free, 5GB-month) |
| Compute (bronze/silver/gold) | `spark-pipelines run` (local Spark 4.1+) | Lakeflow Declarative Pipelines (Databricks Free Edition) | $0 (serverless, fair-use quota) |
| CI | GitHub Actions + docker-compose | — | $0 |
| Deploy (optional) | — | Databricks Jobs API, manual trigger | $0–low, controlled |
| Dashboard | Local Streamlit | Streamlit Community Cloud (optional) | $0 |

No item in this table requires an active credit card beyond the one already tied to the personal GCP account.

---

## 7. Phases

### Phase 1 — Ingestion (producer → messaging → raw bucket)

- **Objective:** capture the Wikipedia change feed and persist it as raw Parquet/Delta, via messaging, in a way that is fully shuttable-down.
- **Input:** `recentchange` SSE stream.
- **Output:** raw files partitioned (by date/hour) in the configured bucket, Parquet or Delta format.
- **Decisions already made:** Pub/Sub Emulator as default; MinIO as default; the producer injects an ingestion timestamp (`_ingested_at`) in addition to the event's `meta.dt` (needed to distinguish event time from processing time in Phase 3).
- **Acceptance criterion (high level):**
  - Given `docker-compose up` executed, When the producer runs for N minutes, Then raw files appear in the bucket with a message count equal to the count of events published to the topic (no loss).
  - Given the environment running, When I run `docker-compose down`, Then no process or leftover cost remains.
- **Full detail:** `SPEC-phase1-ingestion.md`.

### Phase 2 — Bronze (Spark Declarative Pipelines)

- **Objective:** declare the raw files from the bucket as a bronze streaming table (idempotent, reprocessable by construction) via SDP, plus a genuinely external reference dimension (`dim_wiki_reference`) as a batch table refreshed on new-file arrival.
- **Input:** raw Parquet/Delta from Phase 1; a periodic wiki-metadata snapshot from the Wikimedia sitematrix API for `dim_wiki_reference`.
- **Output:** `bronze_recentchange` (streaming table, append-only, CDF enabled) + `bronze_dim_wiki_reference` (batch table).
- **Decisions already made:** implemented as SDP (Python and SQL variants, pick one — convention 9.5.1); no schema transformation beyond typing — bronze mirrors raw faithfully; Change Data Feed enabled from table creation (consumed incrementally by Phase 3).
- **Acceptance criterion (high level):** Given N raw files in the bucket, When `spark-pipelines run` executes (locally or as a Lakeflow pipeline), Then `bronze_count == raw_event_count`, in both environments, without changing pipeline source files.
- **Full detail:** `SPEC-phase2-bronze.md`.

### Phase 2.5 — CI/CD

- **Objective:** automatically validate, on every push to `main`, that the Phase 2 pipeline is still functional against the local stack; deploy to Databricks is manual, not automatic.
- **Input:** `transform_bronze` code + `local-stack/docker-compose.yml`.
- **Output:** green/red GitHub Actions workflow + versioned deploy artifact.
- **Decisions already made:** tests **always** run against the local emulator/MinIO/Spark (zero cost, zero cloud secret dependency); deploy to the Databricks Jobs API sits behind `workflow_dispatch` (manual trigger), avoiding cost or an API call on every commit.
- **Acceptance criterion (high level):** Given a push to main, When CI runs, Then `test_reconciliation.py` passes using only the local stack, with no cloud credential in the default workflow.
- **Full detail:** `SPEC-phase2__5-cicd.md`.

### Phase 3 — Silver (Data Quality + Change Data Feed from bronze)

- **Objective:** consume the **bronze** Change Data Feed incrementally, apply the 6 Data Quality dimensions (accuracy, completeness, consistency, timeliness, validity, uniqueness) as SDP expectations, route invalid rows to a rejects table, and deliver a silver table ready for direct consumption by analysts (dashboards/ad-hoc analysis), while also serving as the source for the Phase 4 dimensional model.
- **Input:** Change Data Feed of `bronze_recentchange` (CDF enabled on bronze, no longer only on silver — see updated Phase 2).
- **Output:** `silver_recentchange` (historical, insert-only, CDF enabled) + `silver_recentchange_rejected`.
- **Modeling:** neither pure Kimball, Inmon, nor Data Vault. Silver follows a Data-Vault-inspired philosophy (historical, never overwritten) without the hub/link/satellite ceremony — not justified for a single source. A Kimball star schema is reserved for when gold is picked up.
- **Decisions already made:** implemented as an SDP pipeline (Python and SQL variants); data contract (`contracts/bronze_recentchange.contract.yaml`, P8) validated structurally as a fail-fast expectation before the DQ expectations; optimized writes avoiding unnecessary shuffle/broadcast (detailed in the phase SPEC).
- **Acceptance criterion (high level):** Given the bronze CDF with new rows, When the pipeline runs, Then `bronze_count = silver_ok_count + silver_rejected_count` (P4 invariant), each rejection has a reason traceable to one of the 6 DQ dimensions, and the pipeline processes only the increment (not the whole table).
- **Full detail:** `SPEC-phase3-silver-cdf.md`.

### Phase 4 — Gold (Dimensional Modeling + Streamlit Dashboard)

- **Objective:** Kimball star schema (`fact_edit_event` + `dim_editor`/`dim_page`/`dim_wiki`/`dim_date`/`dim_change_type`), materialized incrementally as an SDP pipeline, and a Streamlit dashboard for visualization.
- **Input:** Change Data Feed of `silver_recentchange`; `bronze_dim_wiki_reference` (Phase 2) for `dim_wiki` enrichment.
- **Output:** star schema Delta tables + `docs/gold-model-diagram.mmd` (exportable ER diagram) + Streamlit app.
- **Decisions already made:** dimensions (`dim_editor`, `dim_page`, `dim_wiki`) are materialized via SDP's Auto CDC (`create_auto_cdc_flow`/`CREATE FLOW ... AUTO CDC`), which natively supports choosing SCD Type 1 or 2 per dimension — Type 1 is used here (history already preserved upstream in silver); the fact table is a plain append flow, since SCD/CDC semantics don't apply to immutable events; dashboard reads via DuckDB, not Spark.
- **Out of scope for this phase:** feature store (e.g., `gold_editor_features` for contributor churn) and any ML model — left for a dedicated future project, with its own SPEC.
- **Acceptance criterion (high level):** Given a batch from the silver CDF, When the pipeline runs, Then `fact_count == silver_valid_rows_processed` and no foreign key on the fact table is null.
- **Full detail:** `SPEC-phase4-gold-consumption.md`.

---

## 8. Out of scope for the main pipeline (optional appendix)

The items below **are not part of the main pipeline** and should not be implemented unless explicitly requested as a standalone exercise:

- **Relational database CDC (Datastream/Debezium over Postgres):** a conceptually different mechanism (reading a transaction log vs. consuming an already-published feed). If implemented, it should live in its own folder (`appendix/db-cdc/`) with its own SPEC and README explaining the trade-off relative to Phase 1.
- **Apache Beam/Dataflow:** a minimal pipeline via `DirectRunner` could be added as an engine comparison against the Phase 2/3 Spark Structured Streaming, also isolated in `appendix/beam-comparison/`.
- **Feature store and ML models on top of gold** (e.g., `gold_editor_features` for contributor churn, anomaly/vandalism detection): left for a dedicated future project, with its own SPEC — not part of Phase 4 or any current phase.

---

## 9. General conventions

- All Delta tables use snake_case, prefixed by layer: `bronze_`, `silver_`, `gold_`.
- Every component logs structured JSON via stdout (P5), including at minimum: `timestamp`, `component`, `level`, `event_count` (when applicable).
- No phase SPEC may reopen a decision already recorded in this document — only reference it.

### 9.1 Language

- **Python is the default language for the entire implementation** (producer, consumer, Spark transformations, tests, dashboard).
- Other languages are allowed only where they are the task's native tool, never as an alternative to Python: **SQL** (queries/DDL inside notebooks or validation scripts), **Bash** (setup scripts/container entrypoints), **YAML** (configuration — `docker-compose.yml`, GitHub Actions workflows, infrastructure config files).
- **All repository content is written in English** — code, comments, docstrings, SPECs, README, and any other document that ships in the repo. Portuguese is used only in the private study conversation that produced these SPECs, never in anything committed.

### 9.2 Object orientation by integration context

- Every integration with an external service (data source, messaging, storage, database) must be encapsulated in a dedicated *handler* class, named `{Service}Handler` — e.g., `WikiEventsHandler` (consumes the Wikipedia SSE), `PubSubEmulatorHandler`, `S3CompatibleStorageHandler`.
- A handler is responsible exclusively for communication with that service (connection, retries, payload parsing). Business logic (transformation, validation, rules) never lives inside the handler — the handler only knows how to "talk" to the service.
- Handlers implement the Section 4 interfaces (`MessagingBackend`, `StorageBackend`, `ComputeBackend`) when applicable — the handler class is the concrete implementation of the contract, not a separate object.

### 9.3 DRY (Don't Repeat Yourself)

- Logic implemented once is reused, never duplicated by copy-paste across phases. Code shared between phases (e.g., event schema parsing, structured logging functions, storage URI resolution) lives in `src/shared/` and is imported by every phase that needs it.
- Before writing a new function, Claude Code should check whether an equivalent one already exists in `src/shared/` or in an existing handler.

### 9.4 KISS (Keep It Simple) and readability

- Functions should have **at most ~50 lines** whenever possible; if a function exceeds that, it's a sign it should be split into smaller, descriptively named functions.
- Function and variable names must be self-explanatory enough that the logic can be understood without needing an extra comment (e.g., `parse_recentchange_event()`, not `process()`).
- Complex logic (e.g., Phase 3 validation rules, bronze/silver reconciliation) should be broken into named, sequential steps, not a single monolithic function — the goal is for the code to serve as learning-review material, not just a functional implementation.

### 9.5 Handlers vs. Pipelines — where imperative code ends and SDP begins

- **Handler** (convention 9.2): integrates with an external service (SSE, messaging, storage). One per service, with no hierarchy between them — each talks to something different. This remains hand-written imperative Python, because Phase 1 (producer/consumer) isn't a Spark job at all — SDP has nothing to offer there.
- **Pipeline** (Phases 2, 3, 4): declared, not orchestrated by hand. Each phase's `bronze`/`silver`/`gold` tables are defined as SDP datasets (`@dp.table`/`@dp.materialized_view`/`@dp.append_flow`/`@dp.create_auto_cdc_flow` in Python, or `CREATE OR REFRESH STREAMING TABLE`/`CREATE FLOW` in SQL) — Spark computes the dependency DAG and incremental execution itself. There is no custom base class to maintain here; that responsibility now belongs to the SDP runtime, not to project code. (Earlier drafts of this SPEC described a hand-rolled `BaseDeltaIngestor` class hierarchy for this — superseded by this decision; see `docs/trade-offs.md`.)
- **Why this split still respects Liskov/SRP:** a Handler and a pipeline dataset definition are different *kinds* of thing (an object with methods vs. a declared table + query) — there's no inheritance relationship to misuse here, which is itself a simplification over maintaining a custom `Ingestor` hierarchy.

### 9.5.1 Python vs. SQL for pipeline definitions

Both are provided per phase, side by side — a genuine either/or choice, not a DRY violation, since the goal is letting a human decide which one to keep for the portfolio, not running both simultaneously. Practical guidance for that choice: SQL variants read closer to a data-warehouse background and are shorter for straightforward declarations; Python variants are needed wherever metaprogramming, UDFs, or the DQ engine's programmatic rule-loading (Phase 3) are involved. Once you decide which to keep, delete the other — keeping both indefinitely would violate P0.

### 9.6 Engineering principles in this project

Full definitions in `docs/ENGINEERING-PRINCIPLES.md` (project-agnostic document, reusable across other repositories — covers Principle 0, KISS/YAGNI/DRY, coupling/cohesion, SOLID, stdlib-first, documentation, error handling/logging, Python conventions, and testing). Concrete applications here:

- **YAGNI:** feature store and ML models on top of gold are deliberately out of scope (Section 8) until a real consumer exists — we don't build speculative infrastructure.
- **SOLID (Dependency Inversion):** the Section 4 interface contracts (`MessagingBackend`, `StorageBackend`) are a direct application of this principle — business logic depends on the interface, never on the concrete implementation (MinIO, GCS, Pub/Sub Emulator).
- **YAGNI, again:** keeping both a Python and a SQL variant of every pipeline (convention 9.5.1) indefinitely would be waste — the intent is to pick one after comparing, not maintain two permanently.
- **Robustness Principle:** the `_extra_fields` escape hatch (P8) is the direct application — liberal in what's accepted regarding new/unknown fields, conservative in what's declared/contracted.
- **Stdlib-first:** an external dependency only when the task genuinely requires it (Spark/SDP for distributed processing; Pub/Sub/MinIO SDKs for the pluggable backends) — never for marginal convenience when the standard library would do.
- **Error handling and logging:** P5 (structured logging via stdout) and the fail-fast pattern already used in Phases 2–4 (now expressed as SDP expectations rather than custom checks) are the concrete application of Section 6 of the engineering principles document.

### 9.7 Python environment and dependencies

- **Manager:** `uv`, not `pip`/`venv`/`conda` directly. Virtual environment created as `uv venv .venv --prompt wiki-cdc-streaming`; dependencies added via `uv add <package>` (never manually edited in a `requirements.txt`).
- **Committed file:** `requirements.txt` generated from `pyproject.toml`/`uv.lock` via `uv export --no-hashes --format requirements-txt -o requirements.txt`, updated at the end of every phase that introduces a new dependency. This keeps compatibility with tools that expect `requirements.txt` (CI, Databricks cluster libraries) without giving up `uv`'s deterministic lockfile for development.

### 9.8 Credential management

- **Never hardcode.** Every credential (MinIO keys, `GOOGLE_APPLICATION_CREDENTIALS`, API tokens) lives in `.env`, loaded via `python-dotenv` (`load_dotenv()`) at each component's entry point.
- `.env.example` is committed (template with variable names and placeholder values); the real `.env` is never committed — covered by `.gitignore`.
- This convention applies to every variable already listed in the "Configuration" sections of each phase SPEC — none of them should appear hardcoded in code, even the non-sensitive ones (keeps a single standard, no case-by-case exception).

---

## 10. Portfolio deliverables

Beyond the technical SPECs, the repository carries documents aimed at whoever evaluates the project on GitHub (recruiter, technical interviewer, or anyone reproducing the project). Each has a specific purpose — avoiding content duplication between them is itself an application of DRY (P0/`docs/ENGINEERING-PRINCIPLES.md`):

| Document | Purpose | Should not contain |
|---|---|---|
| `README.md` | Project objective, technologies used, general architecture diagram, high-level architectural decisions — what someone reads in 3 minutes to understand the project | Detailed execution steps (that's `RUNBOOK.md`); extensive justification of each trade-off (that's `trade-offs.md`) |
| `docs/CLAUDE.md` | **Stable** project memory auto-loaded by Claude Code at the start of every session in this repo (a Claude Code product mechanic, not a portfolio-only artifact): what the project is, where the SPECs live, non-negotiable conventions (P0–P8, uv, `.env`, English-only). Changes slowly, across phases | Volatile per-session status (that's `docs/PROGRESS.md`); decision history (that's `trade-offs.md`) |
| `docs/RUNBOOK.md` | Strictly detailed step-by-step execution — local and cloud — with an explicit cost warning at every cloud step, and a ready-to-paste code block showing the cloud alternative where it differs from local (e.g., registering a table in Unity Catalog instead of a direct Delta path) | Design justification (that's `trade-offs.md`); any real credential |
| `docs/trade-offs.md` | Decision → alternatives → why → what was sacrificed, per phase | Execution instructions |
| `docs/ENGINEERING-PRINCIPLES.md` | DRY/KISS/YAGNI/SOLID/Principle 0, in a project-agnostic way | Any mention of Wikipedia, Delta, or a decision specific to this repository |
| `docs/implementation-workflow.md` | Repeatable per-phase implementation process with Claude Code (branch → session → incremental review → local verification → commit → PR → merge) | Phase-specific commands (already covered in each SPEC's "Operating commands") |
| `docs/PROGRESS.md` | **Volatile** durable text checkpoint between sessions, refreshed at the end of every session (Section 11) | Decision history (that's `trade-offs.md`); stable conventions (that's `docs/CLAUDE.md`) |

### 10.1 Why cloud code blocks live in documentation, not in source code

Given principle P2 (swap via configuration), the source code is already agnostic by design — there is no separate "cloud code path" hidden behind a comment. What actually changes to run in the cloud, beyond configuration, is *setup* that only exists once (e.g., catalog/schema creation DDL in Unity Catalog) — that doesn't belong in the versioned pipeline, so it lives as a ready-to-use reference block in `RUNBOOK.md`, not as commented-out code inside the `.py` files. Keeping dead/commented code in the source would violate KISS and P0.

### 10.2 When each document is written — incremental, not end-of-project

`README.md` and `docs/CLAUDE.md` are **not** written once at the end. A repository sitting at Phase 2 with no README beyond the LICENSE looks abandoned, not in-progress — bad for a portfolio repo where each phase is merged to `main` via its own PR and could be viewed by anyone at any point.

- `README.md`: a minimal skeleton (objective, tech stack placeholder, architecture diagram placeholder, a phase-status checklist) is created during one-time setup, before Phase 1. Each phase's PR updates the relevant section (status checklist, tech stack additions, architecture diagram once it exists) as part of that same PR — not a separate end-of-project task.
- `docs/CLAUDE.md`: a baseline version is also created during one-time setup (project one-liner, pointers to `SPEC-agnostic-architecture.md` and `ENGINEERING-PRINCIPLES.md`, the non-negotiable conventions). Each phase appends a short "what exists now" note at merge time — see `docs/implementation-workflow.md`.
- `docs/trade-offs.md` and `docs/RUNBOOK.md` are populated per phase too, as each phase's decisions and execution steps become concrete — never written speculatively ahead of the phase that produces the content (YAGNI). This isn't just a stated intention: the implementation prompt template in `docs/implementation-workflow.md` explicitly instructs Claude Code to fill in that phase's `docs/RUNBOOK.md` section before the phase is considered done — it's a deliverable of the phase, not a follow-up request.

---

## 11. Development continuity (Claude Code usage limits)


This project is implemented across multiple Claude Code sessions, possibly over days/weeks. Two distinct risks can interrupt a session, and each deserves a different treatment:

**Usage limit (session/weekly) — not lost work.** Claude Code sessions are saved locally per project directory; upon hitting a limit, Claude Code blocks new requests until the reset time shown in the error itself, but the entire conversation history and any file already written/committed remains intact. Resuming with `claude --continue` (most recent session) or `claude --resume <name>` (named session) restores the conversation exactly where it left off, including tool calls already made.

**Recommended practice — name the session by phase:** when starting the implementation of each SPEC, name the session after the phase (`claude -n phase3-silver`). This makes it trivial to resume the right session later (`claude --resume phase3-silver`) when several phases have parallel or paused sessions.

**The checkpoint that actually matters is the SDD process, not the Claude Code session.** Each phase is already designed as a small, testable unit (Section 7): Given/When/Then acceptance criteria + automated tests + a Git commit. This means that, in the worst case (lost session, weekly limit exhausted for days, or even a machine switch), resuming doesn't depend on conversation memory — it depends on the repository state: which test is passing, which SPEC is being implemented, what the last commit was. A brand-new session, with no history at all, can orient itself just by looking at `git log` + the current phase's SPEC + `pytest` output.

**Recommended practice — `docs/PROGRESS.md`:** updated at the end of every work session with three lines: current phase, what's already implemented, what's left. This is the durable text checkpoint — it works even if the Claude Code session history expires (default 30-day retention) or if implementation continues in a different named session.

**Context management within a long session:** a separate risk from the usage limit — automatic compaction (`/compact`) already prevents most cases; `/clear` preserves the previous conversation (resumable later) and frees up space to continue. Running `/usage` at the start of a longer implementation session (e.g., Phase 3, the densest one) helps decide whether it's worth splitting the work into more than one session before starting.

## 12. Next documents

1. `SPEC-phase1-ingestion.md`
2. `SPEC-phase2-bronze.md`
3. `SPEC-phase2__5-cicd.md`
4. `SPEC-phase3-silver-cdf.md`
5. `SPEC-phase4-gold-consumption.md`
