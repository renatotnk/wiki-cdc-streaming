"""silver_recentchange -- the phase deliverable analysts query directly.
Reads the already-flattened/normalized/deduped/tagged
silver_recentchange_staging (see that file) and keeps only rows that passed
all 4 traceable blocking DQ dimensions (completeness/validity/accuracy/
consistency -- pipelines/silver/dq_rules.py). Uniqueness is already enforced
upstream in staging (dropDuplicatesWithinWatermark); timeliness never blocks
(SPEC-phase3-silver-cdf.md Section 5) -- both dimensions play no role in this
filter.

A plain incremental streaming read of staging, not a CDF read: staging is
append-only (each row lands in exactly one of silver_recentchange /
silver_recentchange_rejected, never updated/deleted), so a plain read is
already equivalent to consuming its CDF and needs no extra option.

Selects contracts/silver_recentchange.contract.yaml's exact column list
explicitly, rather than `.drop("_dq_failure_reasons")` off of staging's full
output -- self-documenting, and avoids needing staging's *entire* column
list just to compute "all columns except this one".

Like silver_recentchange_staging.py, this table can only be added to the
pipeline spec (and run) *after* silver_recentchange_staging_py has already
been materialized by an earlier run -- see that file's docstring for the
full, empirically-verified explanation (SDP resolves every file's query
during registration, before any of them execute, so a table declared for
the first time in the same run doesn't exist yet when a sibling tries to
read it).
"""

from dotenv import load_dotenv
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import size

load_dotenv()

spark = SparkSession.active()

# Matches contracts/silver_recentchange.contract.yaml's `fields:` list, in
# order -- see this file's docstring for why this is an explicit `.select`,
# not a `.drop("_dq_failure_reasons")` off of staging's full output.
_CONTRACT_COLUMNS = [
    "_event_id",
    "type",
    "title",
    "user",
    "bot",
    "wiki",
    "timestamp",
    "server_url",
    "meta_dt",
    "length_old",
    "length_new",
    "_ingested_at",
    "_producer_instance",
    "_extra_fields",
    "_schema_version",
    "_ingestion_latency_seconds",
    "_is_late_arrival",
    "_silver_loaded_at",
    "dt",
    "hour",
]


@dp.table(
    name="silver_recentchange_py",  # rename to silver_recentchange once chosen (convention 9.5.1)
    table_properties={"delta.enableChangeDataFeed": "true"},  # consumed by Phase 4 (gold)
    partition_cols=["dt", "hour"],
)
def silver_recentchange_py():
    staging = spark.readStream.format("delta").table("silver_recentchange_staging_py")
    return staging.filter(size("_dq_failure_reasons") == 0).select(*_CONTRACT_COLUMNS)
