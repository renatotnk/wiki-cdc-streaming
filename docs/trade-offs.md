# Project Technical Trade-offs

This document exists for a specific purpose: to record **why** each architectural decision was made, what alternatives were considered, and what was consciously sacrificed. It's the piece that backs up a technical-interview discussion — every decision here should be defensible out loud, not just "it worked."

Format of each entry: **Decision** → **Alternatives considered** → **Why this and not the others** → **What we accepted losing**.

---

## Data source and ingestion (Phase 1)

### Wikipedia EventStreams instead of relational database CDC

- **Alternatives considered:** self-hosted Postgres + Debezium/Datastream simulating database CDC.
- **Why:** Wikipedia EventStreams is already a change feed published by the source itself — there's no transaction log to read because the change already arrives as an event. Database CDC solves a different problem (a system that *doesn't* expose its changes). Using database CDC here would mean artificially simulating a problem the real source doesn't have.
- **What we accepted losing:** the specific practice of configuring logical replication/binlog and Datastream/Debezium network connectivity. Recorded as an optional appendix (`appendix/db-cdc/`), isolated from the main pipeline, so as not to pay that complexity cost in the core architecture.

### Pub/Sub Emulator as default, real Pub/Sub as optional

- **Alternatives considered:** Redpanda/Kafka (more aligned with what the industry uses outside the GCP ecosystem); going straight to real Pub/Sub.
- **Why:** the project is already GCP-first (BigQuery, Datastream named in the study plan); keeping Pub/Sub as the concept, but via an emulator, preserves learning the right tool with no development cost. Switching to real Pub/Sub is just configuration (principle P2), so the Kafka/Redpanda "lesson" isn't entirely lost — it's documented here as a path not taken.
- **What we accepted losing:** direct exposure to the Kafka Connect ecosystem, which is the most common pairing with Debezium outside GCP.

### Parquet for raw, not Delta

- **Alternatives considered:** Delta Lake already at the raw layer.
- **Why:** raw is a faithful, ephemeral dump — it doesn't need Delta's transaction log, time travel, or schema enforcement. Delta starts adding real value from bronze onward, where a logical table is actually maintained over time.
- **What we accepted losing:** nothing functional — this just avoids complexity where it doesn't pay off (KISS).

### One `S3CompatibleStorageHandler` for both MinIO and real S3, not two classes

- **Alternatives considered:** a separate `MinioStorageHandler` and `S3StorageHandler`, added when Databricks Free Edition's External Volume limitation (S3-only) made real S3 support necessary.
- **Why:** MinIO speaks the S3 API — the only structural difference from real AWS S3 is a custom endpoint (`fs.s3a.endpoint`) and which credential values are used. Two near-identical classes differing only by an optional endpoint would be duplication for its own sake. One class, parametrized by whether `S3_ENDPOINT_URL` is set, covers both `STORAGE_BACKEND=minio` and `STORAGE_BACKEND=s3`.
- **What we accepted losing:** nothing — this was a straightforward DRY win once the S3 requirement surfaced, not a compromise.

### Databricks Free Edition's S3-only External Volumes shape the cloud storage default

- **Alternatives considered:** keeping GCS as the assumed cloud storage pairing for Databricks, as in the project's earlier drafts.
- **Why:** Databricks Free Edition currently only supports External Connections/Volumes backed by S3, not GCS. Once a real Free Edition workspace was set up, this became a hard constraint, not a preference — the practical "cloud" pairing for this project is Databricks Free Edition + S3. GCS remains part of the `StorageBackend` interface (useful if compute ever moves to a non-Databricks Spark environment on GCP) but isn't usable together with Databricks Free Edition specifically.
- **What we accepted losing:** the tighter integration with the personal GCP project (BigQuery, etc.) that was assumed earlier — still usable outside the Databricks Free Edition path, just not the default cloud story for this pipeline anymore.

### No synthetic data anywhere in the pipeline, including dimension tables

- **Alternatives considered:** injecting fabricated/random data into `dim_editor`, `dim_page`, `dim_wiki`, or the source files for `dim_wiki_reference`, to have more control over dimension attribute variety.
- **Why:** every dimension already has a legitimate, non-fabricated source: `dim_editor`/`dim_page` are derived directly from real Wikipedia events; `dim_wiki` is enriched from the real Wikimedia sitematrix API; `dim_date` and `dim_change_type` are deterministically generated (calendar arithmetic and the domain's actual valid-combination enumeration, respectively) — neither is "fake data" in the sense of fabricated content, they're computed from known, real constraints. Introducing synthetic data anywhere would be an unforced complexity with no real benefit, given genuine sources already cover every table.
- **What we accepted losing:** nothing — this is a case where the honest answer ("no synthetic data needed") was also the simplest one (P0).

---

## Bronze (Phase 2)

### Structured Streaming with `Trigger.AvailableNow`, not manual batch nor continuous streaming

**(Superseded — see "Spark Declarative Pipelines instead of hand-rolled Structured Streaming" below. Kept here because the reasoning about batch vs. continuous streaming still applies; SDP just replaces the hand-written orchestration that implemented it.)**

- **Alternatives considered:** partition-parameterized batch `read()` (manual `dt`/`hour`); traditional continuous streaming.
- **Why:** manual batch requires external orchestration deciding "what's new" — information the Structured Streaming checkpoint already keeps natively. Continuous streaming, on the other hand, would leave a process active indefinitely, the opposite of what's wanted in a cost-controlled project. `Trigger.AvailableNow` processes what's available and stops on its own — it gains streaming semantics (incremental discovery, exactly-once via checkpoint) without the operational risk of a job that never shuts down.
- **What we accepted losing:** nothing — this option strictly dominated the other two on the criteria that mattered (cost, correctness, operational simplicity).

### Spark Declarative Pipelines instead of hand-rolled Structured Streaming

- **Alternatives considered:** the custom `BaseDeltaIngestor`/`BronzeIngestor`/`SilverIngestor`/`GoldIngestor` class hierarchy (manual `readStream`/`writeStream`/`Trigger.AvailableNow`/checkpoint wiring) from earlier drafts of this project.
- **Why:** initially, Declarative Pipelines looked like a Databricks-only feature (Delta Live Tables), which would have broken local portability (P1/P6) if adopted as the primary implementation — the plan at the time was to keep it as an appendix. That assumption turned out to be outdated: Spark Declarative Pipelines (SDP) became a genuinely open-source Apache Spark capability starting in Spark 4.1 (`pyspark.pipelines` module, `spark-pipelines` CLI), with Databricks' Lakeflow Declarative Pipelines being the same framework extended for the managed runtime. Once local execution was confirmed real (not an emulation), SDP could become the primary implementation without sacrificing portability — while also directly answering "I want to compare a Python and a SQL implementation side by side," since SDP supports both natively.
- **What we accepted losing:** a Spark 4.1+ / Java 17+ version floor (stricter than the broader JDK 8–17 range that plain PySpark tolerates) — a real, if minor, constraint on the local dev environment.

### Change Data Feed enabled on bronze from table creation

- **Alternatives considered:** enabling CDF only when (and if) needed; enabling it on silver instead of bronze.
- **Why:** Delta's `readStream` fails by default if the source table undergoes any non-append `UPDATE`/`DELETE`/`MERGE` — a real scenario when a corrupted partition needs a one-off manual fix. Enabling CDF up front costs nothing and avoids an expensive migration if that correction happens later. This still holds under SDP — the streaming table's `TBLPROPERTIES`/`table_properties` set it at declaration time either way.
- **What we accepted losing:** nothing measurable — it's low-cost protection against a real maintenance scenario.

### A second, externally-sourced dimension at bronze (`dim_wiki_reference`)

- **Alternatives considered:** keeping all dimensions derived entirely from the fact stream itself, as in the original design.
- **Why:** every dimension being derived purely from `recentchange` events was a bit unrealistic next to a genuine Kimball scenario, where dimensions commonly arrive from independent systems with their own refresh cadence. Sourcing wiki metadata (language, project type) from the Wikimedia sitematrix API, landed as a batch snapshot file and picked up on arrival, adds a real conformed-dimension-from-a-second-source pattern — and doubles as a natural example of SDP's batch/file-arrival semantics.
- **What we accepted losing:** nothing — it's additive, and the added source is genuinely free (a public API, no auth, no cost).

---

## CI/CD (Phase 2.5)

### Test double (`FakeWikiEventsHandler`) instead of real network in tests

- **Alternatives considered:** CI tests connecting for real to Wikipedia's SSE.
- **Why:** real network in CI is fragile (latency, availability, non-determinism) and isn't what's being tested — the handler's contract, not Wikipedia's network. A frozen fixture with known edge cases (a bot event, a deliberate duplicate) is more reliable and faster.
- **What we accepted losing:** test coverage against real changes to Wikipedia's SSE schema — mitigated by manually running against the real source before each development phase, not on every push.

### Manual deploy (`workflow_dispatch`), never automatic on every push

- **Alternatives considered:** automatic deploy to Databricks Jobs on every merge to `main`.
- **Why:** a push unrelated to the pipeline (e.g., a documentation typo fix) shouldn't trigger a Job update or a paid API call. Deploy is an explicit decision, not a side effect of a commit.
- **What we accepted losing:** automatic "continuous deploy" — acceptable because this isn't a production system with an SLA, it's a study/portfolio project.

---

## Silver (Phase 3)

### SQL for Data Quality rules, PySpark only for orchestration/ingestion

**(Superseded — see "SDP expectations instead of hand-rolled DQ SQL files" below.)**

- **Alternatives considered:** everything in PySpark (more uniform); everything in SQL (harder to unit test).
- **Why:** DQ rules are simple, declarative logic — SQL expresses that more directly and is testable in isolation, file by file. Reading the CDF, checkpoint management, and the hybrid write are genuinely complex (state, streaming, conditional decisions) — that justifies PySpark. This split isn't cosmetic, it's a direct application of the "simple in SQL, complex in PySpark" convention.
- **What we accepted losing:** nothing — both worlds remain independently testable.

### SDP expectations instead of hand-rolled DQ SQL files

- **Alternatives considered:** the custom `dq_engine.py` + `dq_rules/*.sql` files, combined manually into `_dq_failure_reasons` via `array_compact`.
- **Why:** once Phase 2/3 moved to Spark Declarative Pipelines, the same 6 DQ dimensions map directly onto SDP's native expectation severities (`expect_or_fail` for contract breaches, `expect_or_drop` for the 5 blocking dimensions, `expect` for the informational timeliness flag) — with dropped-row tracking built into the pipeline's expectation metrics instead of a hand-written split function. This is strictly less custom code for the same guarantee.
- **What we accepted losing:** the DQ rules are no longer trivially portable to a non-SDP Spark job if we ever moved away from SDP — a reasonable bet given SDP's now-confirmed open-source status.

### Uniqueness dedup only within the micro-batch, never an anti-join against all of silver

- **Alternatives considered:** an anti-join against the entire silver table on every run, guaranteeing global uniqueness.
- **Why:** an anti-join against the whole target table would force reading and shuffling the entire history on every incremental batch — exactly the cost that reading via CDF was designed to avoid. It's safe to give this up because three earlier layers of defense already exist (LRU cache in the producer, dedup at the consumer's flush, `SilverIngestor`'s own checkpoint exactly-once guarantee).
- **What we accepted losing:** a formal, automatic *global* uniqueness guarantee — in practice, the residual risk is close to zero given the earlier layers, and the performance gain is substantial.

### Hybrid write (append vs. merge) by `_change_type`

- **Alternatives considered:** always use `MERGE INTO`, for code uniformity and simplicity.
- **Why:** in normal operation, 100% of the batch is insert — paying the shuffle/join cost of a `MERGE` for a case that's always insert would be systematic waste. `MERGE` is only used on the rare path (bronze correction), restricted to the affected keys.
- **What we accepted losing:** code uniformity (two write paths instead of one) — a trade-off accepted because the performance gain on the common path (99%+ of batches) outweighs the marginal simplicity of a single path.

### Data-Vault-inspired silver (historical, insert-only), without hub/link/satellite

- **Alternatives considered:** full Data Vault; Inmon/3NF normalization.
- **Why:** full Data Vault solves integration of multiple heterogeneous sources — not the problem here (single source). We borrow only the philosophy (never overwrite, everything traceable over time via CDF) without the full model's ceremony.
- **What we accepted losing:** nothing relevant for a single source — Data Vault's extra ceremony wouldn't buy any guarantee we don't already have via CDF + insert-only.

### Timeliness as a non-blocking dimension

- **Alternatives considered:** treating lateness as a rejection reason, like the other 5 DQ dimensions.
- **Why:** late data is still valid data — it's the same watermark/late-data logic from streaming theory. Rejecting for lateness would throw away legitimate information; flagging (`_is_late_arrival`) preserves the data and the information about its lateness.
- **What we accepted losing:** nothing — this is strictly the more technically correct decision, not a convenience trade-off.

---

## Gold (Phase 4)

### Kimball star schema, not Data Vault or Inmon

- **Alternatives considered:** keeping everything in Data Vault through to consumption; Inmon normalization.
- **Why:** gold serves direct analytical consumption (dashboards) — exactly the use case Kimball solves. Data Vault/Inmon would add layers of indirection with no benefit for that consumption.
- **What we accepted losing:** nothing — it's the correct paradigm fit for the problem.

### Type 1 SCD on all dimensions, not Type 2

- **Alternatives considered:** Type 2 SCD (as in the Week 1 `dim_customer` exercise).
- **Why:** SCD2 is justified when a business attribute changes and the history of that change matters (e.g., customer address). Here, `user`/`title`/`wiki_code` are essentially stable identity, and the event's own change history is already preserved upstream, in silver, via CDF. Duplicating that responsibility in gold would be redundant.
- **What we accepted losing:** tracking of any dimension attribute that eventually changes (none exists in this model today) — if that need arises, it's a future one-off SCD2, not a general retrofit. With SDP's Auto CDC (see below), that retrofit is now a one-parameter change (`stored_as_scd_type=1` → `2`), not a rewritten merge strategy.

### SDP Auto CDC for dimensions, plain append flow for the fact table

- **Alternatives considered:** hand-written `MERGE INTO` upsert logic for dimensions (the original approach, before adopting SDP); applying Auto CDC to the fact table too, since the request that prompted this used the word "fact table."
- **Why:** Auto CDC (`create_auto_cdc_flow`/`CREATE FLOW ... AUTO CDC`) is a dimensional-modeling tool — it exists to upsert by natural key with a chosen SCD type, which is exactly `dim_editor`/`dim_page`/`dim_wiki`'s shape, not the fact table's. A fact row is an immutable event, never updated once written — applying CDC/SCD semantics to it wouldn't correspond to any real requirement, and would misuse a tool built for a different problem. The fact table uses a plain append flow instead.
- **What we accepted losing:** nothing — this is a correction of scope, not a trade-off with a real cost on either side.

### Hash-based (deterministic) surrogate key, not sequential

- **Alternatives considered:** a centralized incrementing counter (sequence/`IDENTITY`), as in traditional Kimball pipelines.
- **Why:** a centralized sequence generator is a coordination bottleneck in an incremental, distributed streaming context. A hash of the natural key is idempotent by construction — the same input always produces the same key, on any run, with no coordination. This key now feeds directly into Auto CDC's `KEYS` clause rather than a hand-written `MERGE` condition, but the underlying reasoning for choosing hash-over-sequential is unchanged.
- **What we accepted losing:** readable/short surrogate keys (a hash is longer than a sequential integer) — irrelevant for BI/dashboard consumption, which never exposes the surrogate key to the end user.

### No classic Kimball "Unknown Member" pattern

- **Alternatives considered:** a `-1/Unknown` row in each dimension for unresolved keys.
- **Why:** that pattern exists to handle fact and dimension arriving from different source systems, out of order. Here we control the entire pipeline and guarantee, by construction (dimension upsert before fact resolution, within the same micro-batch), that every natural key in the batch already has a surrogate key at resolution time. A null FK would be a sign of a bug, not an expected case.
- **What we accepted losing:** nothing — the classic pattern exists for a multi-source scenario that isn't ours.

### Dashboard via DuckDB, not Spark

- **Alternatives considered:** Streamlit reading via a local SparkSession or connected to the Databricks cluster.
- **Why:** interactive dashboard queries don't need the overhead of spinning up a SparkSession on every page load. DuckDB reads Delta directly (`delta_scan()`) with much lower latency for this access pattern.
- **What we accepted losing:** nothing for the dashboard use case — Spark remains the materialization engine (Phases 2–4), it's just not used to serve interactive reads.

### Feature store and ML explicitly out of scope

- **Alternatives considered:** already including `gold_editor_features` (rolling windows for contributor churn) this round.
- **Why:** YAGNI — building feature-store infrastructure before a concrete ML model consumes it is speculating about a requirement that doesn't exist yet. When that future project exists, it will have its own SPEC, informed by the model's real needs, not guessed at now.
- **What we accepted losing:** nothing — deferring this is the decision that avoids over-engineering, not a technical limitation.

---

## Infrastructure and cost (cross-cutting)

### `e2-micro` Always Free VM instead of Cloud SQL, if/when a relational database is needed (appendix)

- **Alternatives considered:** managed Cloud SQL for the optional database-CDC appendix.
- **Why:** Cloud SQL charges per hour of active instance, even a small one. An `e2-micro` VM sits within GCP's Always Free tier (perpetual, not just a trial), allowing Postgres to run in Docker at genuinely zero cost, including as a source for Datastream via a Forward SSH Tunnel — an officially supported mechanism, not a workaround.
- **What we accepted losing:** the management convenience (automatic backups, managed upgrades) that Cloud SQL would offer — irrelevant for a short-duration study appendix.

### Databricks Free Edition instead of a paid subscription/full trial

- **Alternatives considered:** a paid Databricks trial, Databricks on GCP with active billing.
- **Why:** Free Edition covers portability validation (running the same code locally and on Databricks) at no cost, within a fair-usage quota. Databricks on GCP with real billing is kept as an isolated, optional session, not part of the standard flow.
- **What we accepted losing:** access to a dedicated (non-serverless) cluster and some enterprise features — irrelevant to the goal of validating code portability.
