"""silver_recentchange_staging -- internal fan-out source, not a phase
deliverable in its own right (no contract file, not documented as a table
analysts should query directly). Reads bronze's Change Data Feed once,
flattens/normalizes/dedups/tags it once, and lets
silver_recentchange.py/silver_recentchange_rejected.py each apply a single,
mutually-exclusive filter on the resulting `_dq_failure_reasons` column.

This exists because SDP expectations don't exist in open-source Spark
(pipelines/silver/dq_rules.py's module docstring) -- there is no "expectation
event log" to read rejected rows back out of (SPEC-phase3-silver-cdf.md
Section 8.3's original premise). Splitting a single shared computation by a
plain filter, instead of two independent streaming reads of bronze each
re-doing the dedup/tagging work, makes the P4 reconciliation invariant
(bronze_count = silver_ok_count + silver_rejected_count) hold by
construction: both downstream tables read the exact same already-decided
`_dq_failure_reasons` value per row, computed exactly once.

Python-only, like bronze_recentchange.py: reading bronze's CDF specifically
(as opposed to a plain incremental append read) needs
`.option("readChangeFeed", "true")`, which has no SQL-grammar equivalent in
SDP -- verified empirically, no such clause exists in `CREATE OR REFRESH
STREAMING TABLE`. The SQL variant (silver_recentchange_staging.sql) can only
do a plain incremental read of bronze, which happens to produce identical
results today (bronze is append-only) but would silently stop capturing
future MERGE/UPDATE/DELETE corrections to bronze -- see that file's comment
and docs/SPEC-phase3-silver-cdf.md Section 4 for the SPEC's own callout of
this.

**Discovered constraint, the same kind SPEC-phase2-bronze.md Section
4.1-4.3 already documented for bronze**: `bronze_recentchange` must already
exist -- created by an earlier, *separate* `spark-pipelines run` -- before a
spec that also includes `silver/**` is run for the first time. Verified
empirically: SDP's "Registering graph elements" phase imports and resolves
every file's query *before* "Starting execution" begins for any of them, so
a table declared for the first time in the very same run genuinely doesn't
exist yet (`spark.catalog.tableExists(...)` stays `False` throughout
registration) when a sibling file tries to read it by name -- resolution
fails with `TABLE_OR_VIEW_NOT_FOUND`, regardless of read style (plain vs.
CDF), regardless of `.select()` vs. `.drop()`, regardless of how many
`libraries.glob` entries are involved. Once bronze_recentchange has been
materialized by any prior, separate run, every subsequent run that adds
silver/** resolves and completes normally -- reproduced by running a
bronze-only spec to completion first, then re-running the full bronze+silver
spec, which then succeeds outright. In practice this is rarely a surprise:
Phase 2 is already merged and has already run at least once before Phase 3
exists. It matters for a genuinely first-time setup (a fresh clone, CI) and
for this phase's own test harness (tests/test_silver_pipeline.py runs
bronze-only once before running the combined spec) -- see docs/RUNBOOK.md's
Phase 3 section for the operating-command consequence.
"""

from pathlib import Path

from dotenv import load_dotenv
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, udf
from pyspark.sql.types import StringType

from pipelines.bronze.schema import BRONZE_RECENTCHANGE_SCHEMA
from pipelines.silver.dq_rules import tag_dq_failures, tag_timeliness
from src.shared.contract_loader import required_field_names
from src.shared.text_normalization import normalize_nfc

# Unconditional, not inside `if __name__ == "__main__":` -- spark-pipelines
# *imports* this file as a regular module (see bronze_recentchange.py).
load_dotenv()

spark = SparkSession.active()

REPO_ROOT = Path(__file__).resolve().parents[2]
_BRONZE_CONTRACT_PATH = REPO_ROOT / "contracts" / "bronze_recentchange.contract.yaml"

# Fail-fast structural check (P8) -- not @dp.expect_or_fail, which doesn't
# exist (see module docstring). Raising here crashes spark-pipelines
# registration for the whole graph, exactly as loud as bronze's own
# typing-based fail-fast (SPEC-phase2-bronze.md Section 4).
_missing_contracted_fields = required_field_names(_BRONZE_CONTRACT_PATH) - set(
    BRONZE_RECENTCHANGE_SCHEMA.fieldNames()
)
if _missing_contracted_fields:
    raise RuntimeError(
        "contracts/bronze_recentchange.contract.yaml declares field(s) missing from "
        f"BRONZE_RECENTCHANGE_SCHEMA: {sorted(_missing_contracted_fields)}"
    )

_normalize_nfc_udf = udf(normalize_nfc, StringType())

# bronze_recentchange minus its nested structs (meta, length) and its raw
# `id` (superseded by _event_id), plus dt/hour (path-derived partition
# columns, not in BRONZE_RECENTCHANGE_SCHEMA but present on the Delta table
# itself). An explicit `.select()`, not `.drop("meta", "length", "id")` --
# self-documenting, and avoids needing bronze_recentchange's *entire* column
# list just to compute "all columns except these three".
_BRONZE_PASSTHROUGH_COLUMNS = [
    name for name in BRONZE_RECENTCHANGE_SCHEMA.fieldNames() if name not in ("id", "meta", "length")
]


def _flatten_and_normalize(df):
    # expr("meta.dt") rather than col("meta.dt"): a SQL expression string
    # for nested-field access, kept for clarity (both forms mean the same
    # thing here -- there's no other struct-typed column named literally
    # "meta.dt" to disambiguate against).
    flattened = (
        df.withColumn("meta_dt", expr("meta.dt").cast("timestamp"))
        .withColumn("length_old", expr("length.old"))
        .withColumn("length_new", expr("length.new"))
        .withColumn("title", _normalize_nfc_udf(col("title")))  # NFC before dedup (Section 6/8)
        .withColumn("user", _normalize_nfc_udf(col("user")))
    )
    return flattened.select(
        *_BRONZE_PASSTHROUGH_COLUMNS, "meta_dt", "length_old", "length_new", "dt", "hour"
    )


@dp.table(
    name="silver_recentchange_staging_py",  # rename to silver_recentchange_staging once chosen (convention 9.5.1)
    # No table_properties={"delta.enableChangeDataFeed": "true"} -- nothing
    # downstream needs staging's own CDF, only a plain incremental read
    # (staging is append-only, produced once per micro-batch).
)
def silver_recentchange_staging_py():
    bronze_cdf = spark.readStream.format("delta").option("readChangeFeed", "true").table(
        "bronze_recentchange"
    )
    flattened = _flatten_and_normalize(bronze_cdf)
    # Streaming-safe uniqueness dedup -- NOT ROW_NUMBER() OVER (PARTITION BY
    # ...), which throws NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING on a
    # streaming Dataset (verified empirically). dropDuplicatesWithinWatermark
    # is Spark's own streaming-safe primitive for this, at the cost of not
    # deterministically preferring the highest _ingested_at duplicate, and
    # of not exposing which rows it dropped -- those rows are therefore not
    # traceable in silver_recentchange_rejected (accepted, disclosed
    # limitation, see docs/trade-offs.md).
    #
    # The watermark delay here is deliberately much larger than
    # TIMELINESS_WATERMARK_SECONDS (600s / 10 minutes, dq_rules.py) --
    # discovered the hard way: using the same duration for both means a
    # genuinely late-arriving row (meta_dt far behind _ingested_at, exactly
    # what the timeliness dimension exists to flag) risks being silently
    # state-evicted by dropDuplicatesWithinWatermark *before* tag_timeliness
    # ever sees it, once any more-recent concurrent row advances the
    # watermark past it -- the opposite of "informational, never drops"
    # (Section 5). Dedup-state retention and the timeliness business
    # threshold are two independent concerns; this generously decouples
    # them rather than accidentally coupling them via a shared constant.
    deduped = flattened.withWatermark("meta_dt", "2 hours").dropDuplicatesWithinWatermark(
        ["_event_id"]
    )
    tagged = tag_dq_failures(deduped)
    with_timeliness = tag_timeliness(tagged)
    return with_timeliness.withColumn("_silver_loaded_at", current_timestamp())
