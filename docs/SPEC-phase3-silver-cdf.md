# SPEC — Phase 3: Silver (Data Quality + Change Data Feed from Bronze)

**Depends on:** `SPEC-agnostic-architecture.md` (principles P1–P8, conventions 9.1–9.5), `SPEC-phase1-ingestion.md` (`_extra_fields`/`_schema_version`) and `SPEC-phase2-bronze.md` (CDF enabled on bronze, `bronze_recentchange.contract.yaml` contract, `BaseDeltaIngestor` base class).
**Does not redecide anything already fixed in those documents — only references them.**

---

## 1. Objective

Consume the bronze Change Data Feed incrementally, apply the 6 Data Quality dimensions, isolate invalid rows into a rejects table with a traceable reason, and deliver `silver_recentchange` as a layer **ready for direct consumption by analysts** (dashboards, ad-hoc analysis) — not as an intermediate step toward a gold layer that doesn't exist yet.

## 2. Scope

**In scope:**
- Incremental reading of the bronze CDF (`SilverIngestor`).
- Structural schema assertion against `contracts/bronze_recentchange.contract.yaml` (P8).
- The 6 Data Quality dimensions, expressed mostly in SQL.
- Handling of problematic characters (not to be confused with legitimate multilingual content — Section 6).
- Optimized writes to silver avoiding unnecessary shuffle/broadcast.

**Out of scope:**
- Kimball star schema / dimensional modeling (left for when gold is picked up).
- Any aggregation (`fact_`/`dim_`) — silver is historical at the event grain, not aggregated.

## 3. Data modeling: why no "pure" paradigm

| Paradigm | Why it doesn't fully apply here |
|---|---|
| **Data Vault** | Solves integration of multiple heterogeneous sources via hub/link/satellite. We have a single source — the ceremony of the full model doesn't pay off. |
| **Inmon (CIF)** | Assumes a normalized (3NF) corporate DW integrating multiple concurrent operational systems. Not our scenario. |
| **Kimball** | The natural fit for direct analytical consumption (dashboards), but a star schema with facts/dimensions is **dimensional modeling** work, which belongs to gold — silver doesn't aggregate or dimension yet. |

**Decision:** silver is a **historical, insert-only table at the original event grain** — a philosophy borrowed from Data Vault (never overwritten, everything traceable over time via CDF), without hub/link/satellite. It's deliberately "Kimball-ready": fields that today are attributes of a single table (`user`, `wiki`, `title`, `timestamp`) are exactly the candidates to become dimensions once gold is picked up — but that modeling isn't implemented now (YAGNI: don't build the star schema before there's a BI consumer that needs it).

## 4. Data Contracts in this phase

- **Read:** every batch from the bronze CDF is structurally validated against `contracts/bronze_recentchange.contract.yaml` before any DQ rule. A contracted field missing or with an incompatible type → fail-fast (`ERROR`), the same pattern as Phase 2.
- **Write:** `silver_recentchange` publishes its own contract, `contracts/silver_recentchange.contract.yaml`, including the fields added by this phase (`_dq_failure_reasons`, `_is_late_arrival`, `_ingestion_latency_seconds`) — this is what a future consumer (dashboard, gold) can assume as guaranteed.
- **Schema drift (`_extra_fields`):** a new, non-contracted field is never rejected. This phase only **reports** (an `INFO` log with count and field names) when `_extra_fields` isn't empty in a batch — the decision to promote an extra field to the formal contract is human, not automatic.

## 5. The 6 Data Quality dimensions applied

Five dimensions **block** (row rejected on failure); one is **informational** (never rejects on its own) — an important distinction, detailed below.

| Dimension | Blocks? | Concrete rule | Lives in |
|---|---|---|---|
| **Completeness** | Yes | `_event_id`, `type`, `title`, `wiki`, `meta_dt` cannot be null | SQL |
| **Validity** | Yes | `type` ∈ the contract's enum; title/user with no control character/null byte (Section 6) | SQL |
| **Accuracy** | Yes | `type='new'` implies `length_old` is null or zero; `timestamp` cannot be in the future beyond a 5-minute clock-skew tolerance | SQL |
| **Consistency** | Yes | The partition (`dt`) must match the date derived from `timestamp` | SQL |
| **Uniqueness** | Yes | No duplicate `_event_id` within the same micro-batch (Section 7 explains why we don't compare against the entire silver table) | SQL (window function) |
| **Timeliness** | **No** — informational | `_ingestion_latency_seconds = _bronze_loaded_at - meta_dt`; if above a configurable watermark, flags `_is_late_arrival = true` | SQL, but never generates a rejection |

Timeliness doesn't block because late data is still valid data — it's the same watermark/late-data logic already covered in the streaming theory (Day 8): the correct response to lateness is to flag and decide later (e.g., in a future aggregation window), not to discard.

## 6. Problematic characters vs. legitimate multilingual content — critical distinction

**It is not this phase's goal** to filter non-ASCII content, RTL scripts (Arabic/Hebrew), CJK, or emojis in title/user — that's real, expected Wikipedia data across dozens of languages. What is actually treated as a technical risk:

| Real risk | Detection | Action |
|---|---|---|
| Null byte (`\x00`) | Control-character regex | Rejected (`validity`) |
| Control character outside `\n`/`\t` | Unicode control-range regex | Rejected (`validity`) |
| Invalid UTF-8 sequence | Fails to decode already in Phase 1 (parse) | Never reaches bronze — handled at the source |
| Inconsistent Unicode normalization (NFC vs. NFD) | Byte comparison after normalization | Doesn't reject — normalizes before computing the dedup key (Section 8) |

The last item is, in practice, a disguised **uniqueness** problem: the same title can have two different byte representations because of composed accents, escaping naive deduplication. That's why normalization happens **before** the dedup `ROW_NUMBER()`, not as a separate validity rule.

## 7. Why uniqueness doesn't compare against the entire silver table (a shuffle decision)

Dedup in this phase happens **within each incremental micro-batch**, via `ROW_NUMBER() OVER (PARTITION BY _event_id ORDER BY _ingested_at DESC)`, not via an anti-join against the whole existing silver table. Reason: an anti-join against the entire target table would force reading and shuffling all of silver on every incremental run — exactly the kind of cost that reading via CDF was designed to avoid. This is safe because there are already three earlier layers of defense against a duplicate crossing the batch boundary: the LRU cache in the producer (Phase 1), dedup at the consumer's flush (Phase 1), and the exactly-once guarantee of `SilverIngestor`'s own streaming checkpoint over the CDF (Section 8). Dedup within the batch is the final layer, not the only one.

## 8. `SilverIngestor` (PySpark — complex orchestration, inherits from `BaseDeltaIngestor`)

Per the language convention: reading the CDF, streaming orchestration, and materialization are complex enough to justify PySpark. The DQ rules themselves (Section 5) are simple SQL, invoked by the orchestrator. `SilverIngestor` lives in `src/lib/ingestors/delta_ingestors.py` (convention 9.5), in the same base class as `BronzeIngestor` (Phase 2) — it inherits the `run()` template method and overrides the three steps that change.

```python
# src/lib/ingestors/delta_ingestors.py (continued — same base class as Phase 2)

class SilverIngestor(BaseDeltaIngestor):
    """Inherits read_stream → transform → write_stream → run() from BaseDeltaIngestor.
    DQ rules live in separate .sql files (src/transform_silver/dq_rules/),
    this ingestor only invokes them via dq_engine.py."""

    def read_stream(self) -> DataFrame:
        """readStream over bronze with .option('readChangeFeed', 'true')."""
        ...

    def transform(self, df: DataFrame) -> DataFrame:
        """Transformation pipeline: contract → normalization → DQ.
        Each step is a named function (KISS), not a monolithic function."""
        df = assert_schema_contract(df, contract="bronze_recentchange")  # fail-fast
        df = normalize_text_fields(df)      # NFC via UDF, src/shared/text_normalization.py
        df = apply_dq_rules(df)             # src/transform_silver/dq_engine.py
        return df

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        """Overrides the base default (plain append): hybrid write
        by _change_type — see Section 9. Rejected rows always go through
        plain append, they never need a merge."""
        valid_df, rejected_df = split_valid_rejected(df)
        self._write_rejected(rejected_df)   # always append
        return self._write_valid_hybrid(valid_df)  # append (insert) or restricted merge (rare correction)
```

`IngestorRunResult` (the same generic dataclass from Phase 2, `src/lib/ingestors/utils.py`) is populated here with `input_rows`, `valid_count`, `rejected_count`, `late_arrival_count`, `extra_fields_detected_count` — the fields vary per subclass, the structure is the same.

### 8.1 SQL rule files (DRY — each rule defined once)

```
src/transform_silver/
├── dq_engine.py    # apply_dq_rules(): loads and runs the .sql files below, combines reasons
└── dq_rules/
    ├── completeness.sql
    ├── validity.sql
    ├── accuracy.sql
    ├── consistency.sql
    ├── uniqueness.sql
    └── timeliness.sql
```

Each file receives a temp view (`{batch_view}`) and returns the same columns plus a reason column (`dq_<dimension>_reason`, null if it passed). `apply_dq_rules()` combines all of them via `array_compact(array(...))` into a single `_dq_failure_reasons` column — no rule is reimplemented in Python. This lives in `src/transform_silver/` (not in `lib/ingestors/utils.py`) because it's business logic specific to silver, not generic streaming plumbing (convention 9.5).

## 9. Shuffle and broadcast optimization (this phase's key decision)

- **Hybrid write by `_change_type`:** in normal operation, 100% of the CDF batch is `insert` (bronze only receives appends). These go through a plain `append` — no join, no shuffle against the target table. Only in the rare exception (bronze corrected via `MERGE`/`UPDATE`/`DELETE`, see `SPEC-phase2-bronze.md` Section 4.1) does the `update_postimage`/`delete` subset of the batch go through `MERGE INTO silver`, restricted to the affected keys — never full-table.
- **Explicit broadcast, never implicit:** `spark.sql.autoBroadcastJoinThreshold` is set explicitly (or disabled, `-1`, in the `MERGE` for the exception above) rather than relying on the automatic threshold — this avoids Spark attempting to broadcast a table that has grown beyond expectations. If a small reference table (e.g., a bot allowlist) is added in the future, the `broadcast()` hint is applied manually, never implicitly.
- **No blind `.repartition()`:** partitioning is already inherited from bronze/CDF (`dt`, `hour`). Reducing the file count uses `.coalesce()` (doesn't force a shuffle) instead of `.repartition()` (forces a shuffle) when the only goal is reducing the count of small files.
- **`spark.sql.shuffle.partitions` configurable per environment:** a low value (e.g., equal to the core count) on `local[*]` via `ComputeBackend`; left at default/auto on Databricks. Avoids the overhead of 200 default partitions on a lab-scale dataset running locally.

## 10. Data flow

```
bronze_recentchange (CDF enabled, Phase 2)
        │  SilverIngestor.read_stream()
        ▼
assert_schema_contract()          # fail-fast on contract breach
        │
        ▼
normalize_text_fields()           # NFC, before any dedup (UDF, PySpark)
        │
        ▼
apply_dq_rules()                  # 6 dimensions, SQL, via dq_rules/*.sql
        │
        ▼
split_valid_rejected()
        │                    │
        ▼                    ▼
write_valid()          write_rejected()
(insert append /       (append, always —
 restricted merge       rejects never need
 for corrections)        a merge)
        │                    │
        ▼                    ▼
silver_recentchange   silver_recentchange_rejected
```

## 11. Data contract (silver schema)

Source of truth: `contracts/silver_recentchange.contract.yaml`. Inherits every bronze field (Phase 2, including `_extra_fields`/`_schema_version`), plus:

| New field | Type | Description |
|---|---|---|
| `_dq_failure_reasons` | `array<string>` | Empty in `silver_recentchange`; populated in `silver_recentchange_rejected` |
| `_is_late_arrival` | `boolean` | Informational timeliness — never causes rejection |
| `_ingestion_latency_seconds` | `long` | `_bronze_loaded_at - meta_dt`, in seconds |
| `_silver_loaded_at` | `timestamp` | Moment of processing in this phase |

## 12. Configuration

| Variable | Values | Required |
|---|---|---|
| `COMPUTE_BACKEND` | `local` \| `databricks` (inherited) | Yes |
| `STORAGE_BACKEND` | `minio` \| `gcs` (inherited) | Yes |
| `SILVER_TABLE_PATH` | logical path | Yes |
| `SILVER_REJECTED_TABLE_PATH` | logical path | Yes |
| `SILVER_CHECKPOINT_PATH` | logical path of `SilverIngestor`'s checkpoint | Yes |
| `TIMELINESS_WATERMARK_SECONDS` | int (default: 600) | No |
| `CLOCK_SKEW_TOLERANCE_SECONDS` | int (default: 300) | No |
| `SPARK_SHUFFLE_PARTITIONS` | int (default: number of local cores) | No |

## 13. Code structure for this phase

```
contracts/
└── silver_recentchange.contract.yaml
src/lib/ingestors/
└── delta_ingestors.py        # SilverIngestor(BaseDeltaIngestor) — see Section 8
src/transform_silver/
├── schema.py                 # silver_schema, extends bronze_schema (DRY)
├── dq_engine.py              # apply_dq_rules(): loads and runs dq_rules/*.sql
├── dq_rules/
│   ├── completeness.sql
│   ├── validity.sql
│   ├── accuracy.sql
│   ├── consistency.sql
│   ├── uniqueness.sql
│   └── timeliness.sql
└── cli.py
src/shared/
└── text_normalization.py     # normalize_nfc(), reused by any phase that needs it
```

## 14. Acceptance criteria (Given/When/Then)

1. **Reconciliation invariant**
   Given a batch from the bronze CDF, When `SilverIngestor.run()` executes, Then `input_rows == valid_count + rejected_count` (P4), without exception.

2. **Rejection traceability**
   Given a rejected row, When I inspect `silver_recentchange_rejected`, Then `_dq_failure_reasons` contains at least one of the 5 blocking dimensions, never empty.

3. **Timeliness doesn't block**
   Given an event with `_ingestion_latency_seconds` above the configured watermark, When the job runs, Then the row appears in `silver_recentchange` with `_is_late_arrival = true`, not in rejects.

4. **Schema drift tolerated, contract breach not**
   Given a batch with a new, non-contracted field, When the job runs, Then the row is accepted normally with the field in `_extra_fields` and an `INFO` log; Given a contracted field is missing, When the job runs, Then the job fails with an `ERROR` log (fail-fast).

5. **Deduplication without duplicating normalized characters**
   Given two events with the same `_event_id` but the title represented in different NFC and NFD forms, When `normalize_text_fields()` runs before dedup, Then only one row survives.

6. **Multilingual content is not affected**
   Given events with titles in Arabic, Chinese, and with an emoji, When the `validity` rules run, Then none of these rows is rejected because of language/script (only for a real null byte/control character, if present).

7. **Write with no unnecessary shuffle**
   Given a 100% `insert` batch (normal case), When `write_stream()` executes, Then the execution plan (`.explain()`) contains no `MERGE`/join step against the target table — only `append`.

8. **Portability**
   Given the same `SilverIngestor` code, When I run it with `COMPUTE_BACKEND=local` and `COMPUTE_BACKEND=databricks`, Then the result (`valid_count`, `rejected_count`) is identical, with no change to `src/lib/ingestors/` or `src/transform_silver/`.

## 15. Required tests

- `tests/test_dq_rules.py`: each `.sql` file in `dq_rules/` tested in isolation with positive/negative cases (includes a multilingual title as a case that **must pass**, not just rejection cases).
- `tests/test_text_normalization.py`: an NFC/NFD pair of the same text normalizes to the same value.
- `tests/test_silver_ingestor.py`: runs `SilverIngestor` against a local bronze fixture, validates the P4 invariant, and verifies the absence of `MERGE` in the execution plan for the normal case (acceptance criteria item 7). Reuses `tests/test_base_delta_ingestor.py` (Phase 2) to confirm the inherited template method wasn't reimplemented.
- `tests/test_schema_contract.py`: an extra field tolerated vs. a missing contracted field causing a fail-fast.

## 16. Operating commands

```bash
# Process the available increment from the bronze CDF
python -m src.transform_silver.cli

# Inspect rejects locally (reuses the inspect_bronze.py pattern from Phase 2)
python scripts/inspect_silver_rejected.py
```
