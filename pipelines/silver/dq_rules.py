r"""The 6 Data Quality dimensions, as plain PySpark column expressions.

Not `@dp.expect_or_drop`/`@dp.expect`/`@dp.expect_or_fail`: verified
empirically (live against this repo's installed `pyspark==4.1.1`, and
against the official Spark Declarative Pipelines Programming Guide for the
current release, 4.2.0) that SDP expectations do not exist in open-source
Apache Spark -- `pyspark.pipelines` exports no `expect*` symbol at all, and
the SQL `CONSTRAINT ... EXPECT ... ON VIOLATION ...` syntax throws a
`ParseException`. This is the same gap SPEC-phase2-bronze.md Section 4.1
already found and worked around for bronze; SPEC-phase3-silver-cdf.md's
Section 8 sample code wasn't updated to reflect it. See that SPEC's revision
note for the corrected architecture (a shared internal staging table fans
out into silver_recentchange/silver_recentchange_rejected via a plain
filter on `_dq_failure_reasons`, computed once, here).

Built with the DataFrame Column API (`col`/`when`/`rlike`), not a single
`expr("...")` SQL-text string: Spark's SQL string-literal parser silently
drops backslashes it doesn't recognize as one of its own escape sequences
(`'[\x00-\x08]'` resolves to the literal text `[x00-x08]`, verified
empirically -- a real trap for a hex-escape regex pattern like the one
`validity` needs). Passing the pattern straight to `Column.rlike(pattern)`
skips that SQL-text layer entirely -- the string reaches Java's regex
compiler unmangled, where `\x00`-style hex escapes work as intended.

Uniqueness is deliberately absent from `tag_dq_failures` below: it's
enforced via `dropDuplicatesWithinWatermark` directly on the streaming
DataFrame in silver_recentchange_staging.py, not as a row-wise predicate --
`ROW_NUMBER() OVER (PARTITION BY ...)` throws
`NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING` on a streaming Dataset, and
Spark's own streaming-safe dedup primitive doesn't expose which rows it
dropped, so duplicate _event_id rows removed this way are not traceable in
silver_recentchange_rejected -- a disclosed, accepted limitation (see
trade-offs.md), not an oversight.

The SQL variant (silver_recentchange_staging.sql) hits the exact same
backslash-stripping trap for its own validity check, worked around there by
building the character class from CHR(...) concatenation instead of a
textual `\x` escape -- see that file's comment. Rule logic is otherwise
duplicated as literal SQL, kept in sync by hand: a SQL file can't import
this module, same as dim_wiki_reference.sql already duplicates its schema
(SPEC-phase2-bronze.md Section 7.3/7.4) -- a genuine either/or choice
(convention 9.5.1), not a DRY violation.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    col,
    current_timestamp,
    date_format,
    filter as array_filter,
    from_unixtime,
    lit,
    unix_timestamp,
    when,
)

# Single source of truth for the `type` enum, matching
# contracts/bronze_recentchange.contract.yaml's `type` field (DRY per
# SPEC-agnostic-architecture.md Section 4.4 -- avoids the same enum being
# declared twice between the contract and the DQ rule that enforces it).
VALID_TYPES = ("edit", "new", "log", "categorize")

# Null byte / C0 control characters, excluding tab/newline/carriage return --
# a reject-worthy corruption signal, distinct from legitimate non-ASCII
# scripts/emoji, which must never be rejected (SPEC-phase3-silver-cdf.md
# Section 6).
CONTROL_CHAR_PATTERN = r"[\x00-\x08\x0B\x0C\x0E-\x1F]"

# Defaults, used whenever the pipeline spec's configuration: block doesn't
# set the corresponding spark.wikicdc.* key (e.g. tests/test_dq_expectations.py's
# bare local SparkSession) -- SPEC-phase3-silver-cdf.md Section 9 asks for
# these to be tunable via the pipeline spec's configuration: block, not
# hardcoded, so _clock_skew_tolerance_seconds()/_timeliness_watermark_seconds()
# read them from the active session at call time instead of a fixed constant.
_DEFAULT_CLOCK_SKEW_TOLERANCE_SECONDS = 300
_DEFAULT_TIMELINESS_WATERMARK_SECONDS = 600


def _clock_skew_tolerance_seconds() -> int:
    return int(
        SparkSession.active().conf.get(
            "spark.wikicdc.clock_skew_tolerance_seconds", str(_DEFAULT_CLOCK_SKEW_TOLERANCE_SECONDS)
        )
    )


def _timeliness_watermark_seconds() -> int:
    return int(
        SparkSession.active().conf.get(
            "spark.wikicdc.timeliness_watermark_seconds", str(_DEFAULT_TIMELINESS_WATERMARK_SECONDS)
        )
    )


def _completeness_ok() -> Column:
    return col("_event_id").isNotNull() & col("title").isNotNull() & col("wiki").isNotNull() & col(
        "meta_dt"
    ).isNotNull()


def _validity_ok() -> Column:
    valid_type = col("type").isin(*VALID_TYPES)
    no_control_chars = ~col("title").rlike(CONTROL_CHAR_PATTERN)
    return valid_type & no_control_chars


def _accuracy_ok() -> Column:
    length_old_ok = (col("type") != "new") | col("length_old").isNull() | (col("length_old") == 0)
    not_beyond_clock_skew = col("timestamp") <= (
        unix_timestamp(current_timestamp()) + _clock_skew_tolerance_seconds()
    )
    return length_old_ok & not_beyond_clock_skew


def _consistency_ok() -> Column:
    # from_unixtime/date_format resolve against the session's local
    # timezone, not UTC, by default -- verified empirically (a session in
    # America/Sao_Paulo shifted an epoch-second date by a day). raw's
    # dt=/hour= partitioning is derived from meta.dt directly
    # (src/consumer/main.py), which is UTC, so this comparison is only
    # correct if spark.sql.session.timeZone is pinned to UTC --
    # pipelines/spark-pipeline.yml.example does this pipeline-wide.
    return col("dt") == date_format(from_unixtime(col("timestamp")), "yyyy-MM-dd")


# Order matches SPEC-phase3-silver-cdf.md Section 5's table (minus
# uniqueness/timeliness -- see module docstring).
_BLOCKING_DIMENSIONS = {
    "completeness": _completeness_ok,
    "validity": _validity_ok,
    "accuracy": _accuracy_ok,
    "consistency": _consistency_ok,
}


def tag_dq_failures(df: DataFrame) -> DataFrame:
    """Adds `_dq_failure_reasons` (array<string>): the name of every
    blocking dimension this row fails, or an empty array if it fails none.
    Row-wise only (no window function) -- safe on a streaming DataFrame.
    """
    reasons = [when(~check(), lit(name)) for name, check in _BLOCKING_DIMENSIONS.items()]
    return df.withColumn(
        "_dq_failure_reasons", array_filter(array(*reasons), lambda x: x.isNotNull())
    )


def tag_timeliness(df: DataFrame) -> DataFrame:
    """Adds `_ingestion_latency_seconds` and `_is_late_arrival`. Informational
    only -- SPEC-phase3-silver-cdf.md Section 5 -- never blocks, never
    contributes to `_dq_failure_reasons`.
    """
    latency = unix_timestamp(col("_ingested_at").cast("timestamp")) - unix_timestamp(col("meta_dt"))
    return df.withColumn("_ingestion_latency_seconds", latency).withColumn(
        "_is_late_arrival", col("_ingestion_latency_seconds") > _timeliness_watermark_seconds()
    )
