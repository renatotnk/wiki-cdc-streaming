# SPEC — Phase 3: Silver (Data Quality + Change Data Feed from Bronze, via SDP)

**Depends on:** `SPEC-agnostic-architecture.md` (principles P1–P8, conventions 9.1–9.5.1), `SPEC-phase1-ingestion.md` (`_extra_fields`/`_schema_version`) and `SPEC-phase2-bronze.md` (CDF enabled on bronze, `bronze_recentchange.contract.yaml` contract, SDP pipeline spec).
**Does not redecide anything already fixed in those documents — only references them.**

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

> **To validate during implementation:** the code in Section 8 reads `bronze_recentchange` as a plain stream. In normal operation (bronze only ever appends) this is equivalent to reading its CDF, since every commit is an insert. If bronze is ever corrected via `MERGE`/`UPDATE`/`DELETE` (Section 4.1 of `SPEC-phase2-bronze.md`), confirm whether SDP requires an explicit CDF read option (analogous to `.option("readChangeFeed", "true")` in raw Structured Streaming) to consume that correction correctly rather than erroring — check current SDP documentation for the exact mechanism at implementation time, since this is a newer part of the API that may still be evolving.

## 5. The 6 Data Quality dimensions as SDP expectations

Five block (row routed to `silver_recentchange_rejected` on failure); timeliness is informational (never blocks) — same distinction as before, now expressed through SDP's three expectation severities instead of custom SQL:

| Dimension | Expectation type | Rule |
|---|---|---|
| **Completeness** | `expect_or_drop` (Python) / `ON VIOLATION DROP ROW` (SQL) | `_event_id`, `type`, `title`, `wiki`, `meta_dt` not null |
| **Validity** | `expect_or_drop` | `type` in the contract's enum; no control character/null byte in title/user (Section 6) |
| **Accuracy** | `expect_or_drop` | `type='new'` implies `length_old` null/zero; `timestamp` not beyond a 5-minute clock-skew tolerance in the future |
| **Consistency** | `expect_or_drop` | Partition `dt` matches the date derived from `timestamp` |
| **Uniqueness** | `expect_or_drop` | No duplicate `_event_id` within the same micro-batch (Section 7 — same shuffle reasoning as before, unaffected by the SDP move) |
| **Timeliness** | `expect` (Python) / `EXPECT` with no `ON VIOLATION` clause (SQL) — logs but never drops | `_ingestion_latency_seconds` above watermark → `_is_late_arrival = true` |

SDP's `expect_or_drop`/`ON VIOLATION DROP ROW` is exactly the "route to a side table" pattern we need for the 5 blocking dimensions — dropped rows are captured via SDP's expectation metrics and written to `silver_recentchange_rejected` in the same flow (Section 7), preserving the P4 invariant without hand-rolled row-splitting logic.

## 6. Problematic characters vs. legitimate multilingual content

Unchanged from the prior draft — see the table in the previous version of this SPEC section for the exact distinction (null byte/control character = reject; non-ASCII/RTL/CJK/emoji = never reject; NFC/NFD inconsistency = handled as a uniqueness concern via normalization before dedup, Section 8).

## 7. Why uniqueness still doesn't compare against the entire silver table

Unchanged reasoning: dedup happens **within each incremental micro-batch** via `ROW_NUMBER() OVER (PARTITION BY _event_id ORDER BY _ingested_at DESC)`, relying on the same three earlier defense layers (producer LRU cache, consumer flush dedup, and now SDP's own incremental/checkpoint exactly-once guarantee on the CDF read) instead of an expensive anti-join against all of silver.

## 8. Pipeline definitions

### 8.1 Python (`pipelines/silver/silver_recentchange.py`)

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import expr

@dp.table(name="silver_recentchange", table_properties={"delta.enableChangeDataFeed": "true"})
@dp.expect_or_fail("contract_valid", "_event_id IS NOT NULL AND type IS NOT NULL")  # P8, fail-fast
@dp.expect_or_drop("completeness", "_event_id IS NOT NULL AND title IS NOT NULL AND wiki IS NOT NULL AND meta_dt IS NOT NULL")
@dp.expect_or_drop("validity", "type IN ('edit', 'new', 'log', 'categorize') AND NOT rlike(title, '[\\\\x00-\\\\x08\\\\x0B\\\\x0C\\\\x0E-\\\\x1F]')")
@dp.expect_or_drop("accuracy", "(type != 'new' OR length_old IS NULL OR length_old = 0) AND timestamp <= unix_timestamp(current_timestamp()) + 300")
@dp.expect_or_drop("consistency", "dt = date_format(from_unixtime(timestamp), 'yyyy-MM-dd')")
@dp.expect("timeliness_flag", "true")  # informational only — see transform below for the actual flag column
def silver_recentchange():
    df = normalize_text_fields(dp.read_stream("bronze_recentchange"))  # NFC UDF, src/shared/text_normalization.py
    df = df.withColumn(
        "_dedup_rank",
        expr("ROW_NUMBER() OVER (PARTITION BY _event_id ORDER BY _ingested_at DESC)"),
    ).filter("_dedup_rank = 1").drop("_dedup_rank")
    return df.withColumn(
        "_is_late_arrival",
        expr("_ingestion_latency_seconds > 600"),
    )
```

### 8.2 SQL (`pipelines/silver/silver_recentchange.sql`)

```sql
CREATE OR REFRESH STREAMING TABLE silver_recentchange
(
  CONSTRAINT contract_valid EXPECT (_event_id IS NOT NULL AND type IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT completeness EXPECT (_event_id IS NOT NULL AND title IS NOT NULL AND wiki IS NOT NULL AND meta_dt IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT validity EXPECT (type IN ('edit','new','log','categorize')) ON VIOLATION DROP ROW,
  CONSTRAINT accuracy EXPECT ((type != 'new' OR length_old IS NULL OR length_old = 0)) ON VIOLATION DROP ROW,
  CONSTRAINT consistency EXPECT (dt = date_format(from_unixtime(timestamp), 'yyyy-MM-dd')) ON VIOLATION DROP ROW
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT
  *,
  _ingestion_latency_seconds > 600 AS _is_late_arrival
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY _event_id ORDER BY _ingested_at DESC) AS _dedup_rank
  FROM STREAM bronze_recentchange
) WHERE _dedup_rank = 1;
```

(Text normalization before dedup is a UDF call — in the SQL variant it's registered once via `spark.udf.register("normalize_nfc", ...)` from `src/shared/text_normalization.py` and invoked as `normalize_nfc(title)` in the `SELECT`; the full column list is elided above for readability.)

### 8.3 Rejects flow

Rows dropped by `expect_or_drop`/`ON VIOLATION DROP ROW` are captured via SDP's expectation event log, not manually re-queried — `silver_recentchange_rejected` is populated by a companion flow reading that event log and extracting the failed-constraint name(s) into `_dq_failure_reasons`. This still satisfies the P4 invariant (`bronze_count = silver_ok_count + silver_rejected_count`) without a hand-written split function.

## 9. Contract and configuration

Unchanged from the prior draft — `contracts/silver_recentchange.contract.yaml` (Section 11 of the earlier version) and the same configuration variables (`TIMELINESS_WATERMARK_SECONDS`, `CLOCK_SKEW_TOLERANCE_SECONDS`), now read from the pipeline spec's `configuration:` block instead of raw environment variables inside a custom job.

## 10. Code structure for this phase

```
contracts/
└── silver_recentchange.contract.yaml
pipelines/
└── silver/
    ├── silver_recentchange.py
    ├── silver_recentchange.sql
    └── silver_recentchange_rejected.py   # (or .sql) — reads the expectation event log, see Section 8.3
src/shared/
└── text_normalization.py     # normalize_nfc(), reused across phases (unchanged)
```

## 11. Acceptance criteria (Given/When/Then)

1. **Reconciliation invariant**
   Given a batch from the bronze CDF, When the pipeline runs, Then `input_rows == valid_count + rejected_count` (P4), without exception.

2. **Rejection traceability**
   Given a rejected row, When I inspect `silver_recentchange_rejected`, Then `_dq_failure_reasons` names at least one of the 5 blocking constraints, never empty.

3. **Timeliness doesn't block**
   Given an event above the configured watermark, When the pipeline runs, Then the row appears in `silver_recentchange` with `_is_late_arrival = true`, not in rejects.

4. **Schema drift tolerated, contract breach not**
   Given a new, non-contracted field, When the pipeline runs, Then the row is accepted with the field in `_extra_fields`; given a contracted field missing, When the pipeline runs, Then the `contract_valid` expectation fails the update.

5. **Multilingual content is not affected**
   Given events with titles in Arabic, Chinese, and with an emoji, When the `validity` expectation runs, Then none of these rows is dropped for language/script reasons.

6. **Language-variant equivalence**
   Given the Python and SQL variants, When each runs against the same bronze CDF batch, Then both produce identical `silver_recentchange`/`silver_recentchange_rejected` content.

7. **Portability**
   Given the same pipeline spec, When run locally and on Databricks, Then results are identical with no source-file change.

## 12. Required tests

- `tests/test_dq_expectations.py`: each expectation tested with positive/negative fixtures (includes a multilingual title as a case that **must pass**).
- `tests/test_text_normalization.py`: an NFC/NFD pair normalizes to the same value.
- `tests/test_silver_pipeline.py`: `spark-pipelines dry-run` for both variants, plus a real local run validating the P4 invariant.

## 13. Operating commands

```bash
# Validate (both bronze and silver datasets, same pipeline spec — see Section 8 of SPEC-phase2-bronze.md)
spark-pipelines dry-run --spec pipelines/spark-pipeline.yml

# Run
spark-pipelines run --spec pipelines/spark-pipeline.yml

# Inspect rejects locally
python scripts/inspect_silver_rejected.py
```
