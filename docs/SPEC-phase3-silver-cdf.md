# SPEC — Phase 3: Silver (Data Quality + Change Data Feed from Bronze, via SDP)

**Depends on:** `SPEC-agnostic-architecture.md` (principles P1–P8, conventions 9.1–9.5.1), `SPEC-phase1-ingestion.md` (`_extra_fields`/`_schema_version`) and `SPEC-phase2-bronze.md` (CDF enabled on bronze, `bronze_recentchange.contract.yaml` contract, SDP pipeline spec).
**Does not redecide anything already fixed in those documents — only references them.**

**Revision note:** this SPEC was corrected during implementation after empirically testing against real `pyspark==4.1.1` and a real local MinIO instance — the same kind of correction `SPEC-phase2-bronze.md` already went through for bronze. The original draft's Section 8 assumed SDP expectations (`@dp.expect_or_drop`/`CONSTRAINT ... EXPECT`) exist in open-source Spark; they don't (confirmed live and against the official docs for the current release, 4.2.0) — Phase 2 had already found this for bronze's own fail-fast check, but this SPEC wasn't updated to carry that forward. Sections 4, 5, 7, 8, 9, 11, and 13 were corrected accordingly; see Section 4.1–4.4 below for what changed and why, in the same spirit as `SPEC-phase2-bronze.md` Section 4.1–4.3.

---

## 1. Objective

Consume the bronze Change Data Feed incrementally, apply the 6 Data Quality dimensions as SDP expectations, isolate invalid rows into a rejects table with a traceable reason, and deliver `silver_recentchange` as a layer ready for direct analyst consumption — implemented as a Spark Declarative Pipeline, not hand-rolled Structured Streaming.

## 2. Scope

**In scope:**
- `silver_recentchange` and `silver_recentchange_rejected` as SDP datasets reading bronze's CDF.
- Structural schema assertion against `contracts/bronze_recentchange.contract.yaml` as a fail-fast expectation.
- The 6 Data Quality dimensions, each expressed as an SDP expectation (`@dp.expect*`/`CONSTRAINT ... EXPECT`).
- Handling of problematic characters (Section 6 — same distinction from multilingual content as before).

**Out of scope:**
- Kimball star schema / dimensional modeling (Phase 4).
- Any aggregation — silver is historical at the event grain, not aggregated.

## 3. Data modeling: unchanged reasoning

Same conclusion as before: silver is a **historical, insert-only table at the original event grain**, Data-Vault-inspired without the hub/link/satellite ceremony — a single source doesn't justify the full model. This is unaffected by the move to SDP; only the *mechanism* that enforces insert-only-ness and the DQ rules changes (native pipeline expectations replace custom SQL/PySpark orchestration).

## 4. Data Contracts in this phase

- **Read:** every batch from the bronze CDF is structurally validated against `contracts/bronze_recentchange.contract.yaml` via a fail-on-violation expectation before any DQ expectation runs.
- **Write:** `silver_recentchange` publishes its own contract, `contracts/silver_recentchange.contract.yaml`, unchanged in shape from the prior draft (`_dq_failure_reasons`, `_is_late_arrival`, `_ingestion_latency_seconds`, `_silver_loaded_at`).
- **Schema drift (`_extra_fields`):** unchanged — tolerated, logged as `INFO`, never rejected.

> **Resolved during implementation:** yes, an explicit CDF read option is needed and does differ from a plain incremental read — confirmed both live (installed `pyspark==4.1.1` exposes no `dp.read`/`dp.read_stream` at all; SDP always delegates to a plain `spark.readStream...` call written directly inside the `@dp.table` body) and against the official docs for the current release (4.2.0, same finding). The Python variant therefore calls `spark.readStream.format("delta").option("readChangeFeed", "true").table("bronze_recentchange")` explicitly (see `pipelines/silver/silver_recentchange_staging.py`). The SQL variant has **no way to express this at all** — no SQL clause equivalent to that option exists in this SDP grammar — so `silver_recentchange_staging.sql` only ever sees a plain incremental read of bronze, which happens to equal its CDF today (bronze is append-only) but would silently miss a future `MERGE`/`UPDATE`/`DELETE` correction. This is a real, disclosed asymmetry between the two variants, not an oversight.

### 4.1 Discovered constraint: SDP expectations are not used in this phase either

Same empirical finding `SPEC-phase2-bronze.md` Section 4.1 already made for bronze, re-verified here: `pyspark.pipelines` exports no `expect`/`expect_or_drop`/`expect_or_fail` (confirmed against the installed `pyspark==4.1.1` and the official docs for 4.2.0), and the SQL `CONSTRAINT ... EXPECT ... ON VIOLATION ...` syntax throws a bare `ParseException`. Section 5/8 below are corrected accordingly: the 6 DQ dimensions are expressed as plain PySpark/SQL rule logic (`pipelines/silver/dq_rules.py`), not decorators, and rejects are populated by filtering a computed column rather than by reading an "expectation event log" (which doesn't exist).

### 4.2 Discovered constraint: bronze must already exist before silver is added to the pipeline spec

A same-run reference from one `@dp.table`/SQL streaming table to another — `silver_recentchange_staging` reading `bronze_recentchange`, or `silver_recentchange`/`silver_recentchange_rejected` reading `silver_recentchange_staging` — only resolves if the referenced table was already materialized by an **earlier, separate** `spark-pipelines run`. Verified empirically: SDP's "Registering graph elements" phase imports and resolves every file's query *before* "Starting execution" begins for any of them, so a table declared for the first time in the very same run genuinely doesn't exist yet (`spark.catalog.tableExists(...)` stays `False` throughout registration) — resolution fails with `TABLE_OR_VIEW_NOT_FOUND`, reproduced consistently regardless of read style, `.select()` vs. `.drop()`, or how many `libraries.glob` entries are involved. In practice this is rarely a surprise (Phase 2 is already merged and has already run at least once before Phase 3 exists), but it matters for a genuinely first-time setup and for this phase's own test harness — see Section 13's operating commands and `tests/test_silver_pipeline.py`, both of which run a bronze-only spec once before the combined spec.

### 4.3 Discovered constraint: `CREATE OR REFRESH STREAMING TABLE` doesn't parse — use `CREATE STREAMING TABLE`

The original draft's SQL sample (old Section 8.2) used `CREATE OR REFRESH STREAMING TABLE`, mirroring Databricks Lakeflow documentation. Verified empirically against this installed Spark version: `"OR REFRESH"` only parses for `CREATE MATERIALIZED VIEW` (already used by `dim_wiki_reference.sql`, Phase 2); `CREATE OR REFRESH STREAMING TABLE` throws a bare `ParseException`. The correct syntax for a SQL-declared SDP streaming table here is `CREATE STREAMING TABLE name AS SELECT ... FROM STREAM <sibling_table>` — used throughout Section 8's corrected SQL samples.

### 4.4 Discovered constraint: UDFs cannot be registered from inside a pipeline file

`silver_recentchange_staging.sql` cannot call `normalize_nfc` at all. Registering a Python UDF from inside a file SDP imports — even at plain module level, not inside a query function — throws `[SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.REGISTER_UDF]`; no documented escape hatch was found. This means the SQL variant does not NFC-normalize `title`/`user` at all (Section 6's normalization requirement is Python-only in practice), a real, disclosed correctness gap on top of the uniqueness one below — not a stylistic omission.

## 5. The 6 Data Quality dimensions as SDP expectations

Five block; timeliness is informational (never blocks). Expressed as plain PySpark/SQL rule logic (`pipelines/silver/dq_rules.py`), not SDP expectation decorators, which don't exist (Section 4.1):

| Dimension | Mechanism | Rule |
|---|---|---|
| **Completeness** | row-wise predicate, tagged into `_dq_failure_reasons` | `_event_id`, `type`, `title`, `wiki`, `meta_dt` not null |
| **Validity** | row-wise predicate | `type` in the contract's enum; no control character/null byte in title/user (Section 6) |
| **Accuracy** | row-wise predicate | `type='new'` implies `length_old` null/zero; `timestamp` not beyond a 5-minute clock-skew tolerance in the future |
| **Consistency** | row-wise predicate | Partition `dt` matches the date derived from `timestamp` (requires `spark.sql.session.timeZone=UTC` — Section 9) |
| **Uniqueness** | `dropDuplicatesWithinWatermark` (Python only — Section 7) | No duplicate `_event_id` |
| **Timeliness** | row-wise predicate, informational only, never drops | `_ingestion_latency_seconds` above watermark → `_is_late_arrival = true` |

A shared internal staging table (`silver_recentchange_staging`, Section 8) computes `_dq_failure_reasons` (array<string>, empty when a row fails none of the 4 traceable dimensions) exactly once; `silver_recentchange`/`silver_recentchange_rejected` each apply a single, complementary filter on that column. This is the "route to a side table" pattern the architecture SPEC's P4 invariant needs, without SDP expectations or an "expectation event log" — see Section 8.3.

## 6. Problematic characters vs. legitimate multilingual content

Unchanged from the prior draft: null byte/control character = reject; non-ASCII/RTL/CJK/emoji = never reject; NFC/NFD inconsistency = handled as a uniqueness concern via normalization before dedup (Python variant only — Section 4.4). The control-character regex needs one more piece of care: verified empirically that Spark SQL's string-literal parser silently drops backslashes it doesn't recognize as its own escape sequences (`'[\x00-\x08]'` resolves to the literal text `[x00-x08]`), so a textual `\x`-style hex escape is unsafe both in a `expr("...")` SQL string (Python) and in literal `.sql` text (SQL). The Python variant (`pipelines/silver/dq_rules.py`) avoids this by passing the pattern directly to `Column.rlike(pattern)` (skips the SQL-text layer that mangles it); the SQL variant builds the character class from `CHR(...)` concatenation instead (actual bytes, no backslash to mangle) — see `silver_recentchange_staging.sql`.

## 7. Why uniqueness still doesn't compare against the entire silver table

Dedup happens **within each incremental micro-batch**, relying on the same three earlier defense layers (producer LRU cache, consumer flush dedup, and SDP's own incremental/checkpoint exactly-once guarantee) instead of an expensive anti-join against all of silver — but not via `ROW_NUMBER() OVER (PARTITION BY ...)` as originally drafted: verified empirically that non-time-based window functions are categorically disallowed on a streaming Dataset (`NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING`), regardless of how SDP triggers micro-batches internally. `dropDuplicatesWithinWatermark(["_event_id"])` (after `.withWatermark("meta_dt", "10 minutes")`) is Spark's own streaming-safe primitive for this — at the cost of not deterministically preferring the highest-`_ingested_at` duplicate, and of not exposing which rows it dropped, so duplicate rows removed this way are **not traceable in `silver_recentchange_rejected`** — a disclosed, accepted limitation, kept rare by the three upstream defense layers. The SQL variant has no discoverable SQL-clause equivalent to `dropDuplicatesWithinWatermark` at all (a `WATERMARK ... DELAY OF INTERVAL ...` clause exists, but no paired "drop duplicates within watermark" clause was found) — it does not enforce uniqueness at all, a second, SQL-specific gap on top of this one.

## 8. Pipeline definitions

The corrected architecture fans a single upstream computation out to two tables via a shared, internal staging table — not two independent readers of bronze, and not SDP expectations:

```
bronze_recentchange (CDF)
        │  spark.readStream.format("delta").option("readChangeFeed","true").table(...)  [Python only]
        ▼
silver_recentchange_staging   (internal — no contract, not a phase deliverable)
   - flatten meta.dt -> meta_dt, length.old/new -> length_old/length_new
   - normalize_nfc(title), normalize_nfc(user)     [Python only — Section 4.4]
   - dropDuplicatesWithinWatermark(["_event_id"])  [Python only — Section 7]
   - _dq_failure_reasons (completeness/validity/accuracy/consistency)
   - _ingestion_latency_seconds, _is_late_arrival, _silver_loaded_at
        │
        ├─▶ silver_recentchange           WHERE size(_dq_failure_reasons) = 0
        └─▶ silver_recentchange_rejected  WHERE size(_dq_failure_reasons) > 0
```

### 8.1 Python (`pipelines/silver/silver_recentchange_staging.py`, `silver_recentchange.py`, `silver_recentchange_rejected.py`)

See those files directly for the current, working implementation (rule logic lives in `pipelines/silver/dq_rules.py`, reused by the staging file). Key points not obvious from the code alone:

- The structural fail-fast check (P8) is a plain Python assertion at module-import time (`src/shared/contract_loader.py`), comparing `contracts/bronze_recentchange.contract.yaml`'s field names against `BRONZE_RECENTCHANGE_SCHEMA` — not `@dp.expect_or_fail`, which doesn't exist (Section 4.1).
- `silver_recentchange.py` selects `contracts/silver_recentchange.contract.yaml`'s exact column list explicitly, rather than `.drop("_dq_failure_reasons")` off of staging's output — self-documenting, and avoids needing staging's entire column list just to compute "all columns except this one".
- Per convention 9.5.1's `_py`/`_sql` suffix precedent (`SPEC-phase2-bronze.md` Section 7.2 — two variants can't share a table name in the same pipeline glob), every table here is suffixed accordingly; rename to the unsuffixed name once one variant is chosen and the other deleted.

### 8.2 SQL (`pipelines/silver/silver_recentchange_staging.sql`, `silver_recentchange.sql`, `silver_recentchange_rejected.sql`)

`CREATE STREAMING TABLE`, not `CREATE OR REFRESH STREAMING TABLE` (Section 4.3), and `FROM STREAM <sibling_table>` to read another SDP-declared table as a stream. See those files directly for the current implementation, including the `CHR(...)`-based control-character pattern (Section 6) and the two disclosed SQL-only gaps: no `normalize_nfc` (Section 4.4) and no uniqueness dedup (Section 7).

### 8.3 Rejects flow

`silver_recentchange_rejected` is populated by the complementary filter on `silver_recentchange_staging`'s `_dq_failure_reasons` column (`size(...) > 0`), computed once upstream — not by reading an "expectation event log", which doesn't exist (Section 4.1). This satisfies the P4 invariant for the 4 traceable dimensions exactly, by construction (same deterministic column, same staging table, mutually exclusive filters); uniqueness is the one disclosed exception (Section 7).

## 9. Contract and configuration

`contracts/silver_recentchange.contract.yaml` — see that file, structured like `contracts/bronze_recentchange.contract.yaml` (Section 4.4 of the architecture SPEC). Configuration values (`spark.wikicdc.timeliness_watermark_seconds`, default 600; `spark.wikicdc.clock_skew_tolerance_seconds`, default 300) are read from the pipeline spec's `configuration:` block via `SparkSession.active().conf.get(key, default)` inside `pipelines/silver/dq_rules.py`, falling back to the default when unset (e.g. in unit tests, which don't render a full pipeline spec). One more real config value belongs here, not originally anticipated: `spark.sql.session.timeZone: UTC` — `date_format`/`from_unixtime` resolve against the session's local timezone by default, not UTC (verified empirically: a session in `America/Sao_Paulo` shifted an epoch-second date by a day), which would silently break the consistency dimension on any machine not already in UTC. Pinned pipeline-wide in `pipelines/spark-pipeline.yml.example`'s `configuration:` block (a real, dynamically-settable Spark config, not a "static" JVM-boot-time one).

## 10. Code structure for this phase

```
contracts/
└── silver_recentchange.contract.yaml
src/shared/
├── text_normalization.py     # normalize_nfc()
└── contract_loader.py        # required_field_names() -- P8 fail-fast check, Section 8.1
pipelines/
└── silver/
    ├── dq_rules.py                        # the 6 DQ dimensions as PySpark rule functions
    ├── silver_recentchange_staging.py      # / .sql -- internal fan-out source, Section 8
    ├── silver_recentchange.py              # / .sql
    └── silver_recentchange_rejected.py     # / .sql
src/shared/
└── text_normalization.py     # normalize_nfc(), reused across phases (unchanged)
```

## 11. Acceptance criteria (Given/When/Then)

1. **Reconciliation invariant**
   Given a batch from the bronze CDF, When the pipeline runs, Then `input_rows == valid_count + rejected_count` (P4), without exception.

2. **Rejection traceability**
   Given a rejected row, When I inspect `silver_recentchange_rejected`, Then `_dq_failure_reasons` names at least one of the 4 traceable blocking dimensions (completeness/validity/accuracy/consistency), never empty. Uniqueness is the one disclosed exception (Section 7) — a duplicate `_event_id` removed by `dropDuplicatesWithinWatermark` is not traceable here.

3. **Timeliness doesn't block**
   Given an event above the configured watermark, When the pipeline runs, Then the row appears in `silver_recentchange` with `_is_late_arrival = true`, not in rejects.

4. **Schema drift tolerated, contract breach not**
   Given a new, non-contracted field, When the pipeline runs, Then the row is accepted with the field in `_extra_fields`; given a contracted field missing, When the pipeline runs, Then the module-level fail-fast assertion (`src/shared/contract_loader.py`, not `@dp.expect_or_fail` — Section 4.1) raises and crashes registration.

5. **Multilingual content is not affected**
   Given events with titles in Arabic, Chinese, and with an emoji, When the `validity` dimension is evaluated, Then none of these rows is tagged for language/script reasons.

6. **Language-variant equivalence**
   Given the Python and SQL variants, When each runs against the same bronze CDF batch, Then both produce the same `_event_id` set in `silver_recentchange`/`silver_recentchange_rejected` — not byte-identical row counts, since the SQL variant doesn't dedup at all (Section 7) and doesn't NFC-normalize (Section 4.4), both disclosed gaps.

7. **Portability**
   Given the same pipeline spec, When run locally and on Databricks, Then results are identical with no source-file change — except the two disclosed Python/SQL asymmetries above, which are inherent to the language choice, not the environment.

## 12. Required tests

- `tests/test_dq_expectations.py`: each dimension tested with positive/negative fixtures against `pipelines/silver/dq_rules.py` directly (includes a multilingual title as a case that **must pass**) — no `spark-pipelines` subprocess needed, since the rule logic is plain functions, not decorators.
- `tests/test_text_normalization.py`: an NFC/NFD pair normalizes to the same value.
- `tests/test_silver_pipeline.py`: `spark-pipelines dry-run` and a real local run for both variants, validating the P4 invariant, rejection traceability, multilingual survival, late-arrival flagging, and Python/SQL `_event_id`-set equivalence. Both bootstrap with a bronze-only spec first (Section 4.2) before running the combined spec.

## 13. Operating commands

```bash
# One-time bootstrap, or whenever bronze_recentchange doesn't already exist
# (Section 4.2): a spec with only the bronze/** library glob, run for real
# (not dry-run — dry-run doesn't materialize anything either).
spark-pipelines run --spec pipelines/spark-pipeline.bronze-only.yml

# From then on, validate and run the combined spec normally
spark-pipelines dry-run --spec pipelines/spark-pipeline.yml
spark-pipelines run --spec pipelines/spark-pipeline.yml

# Inspect rejects locally
python scripts/inspect_silver_rejected.py
```
