"""The 6 DQ dimensions -- SPEC-phase3-silver-cdf.md Section 12.

Named test_dq_expectations.py per that SPEC's own required-tests list, even
though the underlying mechanism is plain PySpark rule functions
(pipelines/silver/dq_rules.py), not SDP `@dp.expect*` decorators -- those
don't exist in open-source Spark (see dq_rules.py's module docstring).
Exercised directly against small batch DataFrames built with a local
SparkSession -- no `spark-pipelines` subprocess needed, since
`tag_dq_failures`/`tag_timeliness` are plain functions, not pipeline
decorators.
"""

from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from pipelines.silver.dq_rules import (
    _DEFAULT_TIMELINESS_WATERMARK_SECONDS as TIMELINESS_WATERMARK_SECONDS,
    tag_dq_failures,
    tag_timeliness,
)

# Explicit schema, not inferred: a single-row DataFrame with a None override
# (e.g. title=None for the completeness fixtures) gives Spark's schema
# inferer nothing else to compare against, so it can't determine that
# column's type at all (CANNOT_DETERMINE_TYPE) -- same pitfall
# tests/test_bronze_pipeline.py's _write_raw_fixture already documents.
ROW_SCHEMA = StructType(
    [
        StructField("_event_id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("wiki", StringType(), True),
        StructField("meta_dt", TimestampType(), True),
        StructField("length_old", LongType(), True),
        StructField("length_new", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("dt", StringType(), True),
        StructField("_ingested_at", StringType(), True),
    ]
)

BASE_ROW = {
    "_event_id": "1",
    "type": "edit",
    "title": "Test page",
    "wiki": "enwiki",
    "meta_dt": datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc),
    "length_old": 100,
    "length_new": 120,
    "timestamp": int(datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc).timestamp()),
    "dt": "2026-08-01",
    "_ingested_at": "2026-08-01T10:00:05+00:00",
}


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test_dq_expectations").getOrCreate()
    # date_format/from_unixtime resolve against the session's local timezone,
    # not UTC, by default -- verified empirically (e.g. America/Sao_Paulo
    # shifts an epoch-second date by a day). Every timestamp in this project
    # (meta.dt, _ingested_at) is UTC, and raw's dt=/hour= partitioning is
    # derived from meta.dt directly (src/consumer/main.py), so the
    # consistency dimension must compare against a UTC-derived date
    # regardless of the machine's local default -- pinned here the same way
    # pipelines/spark-pipeline.yml.example pins it for the real pipeline.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _row(spark, **overrides):
    data = BASE_ROW | overrides
    return spark.createDataFrame([Row(**data)], schema=ROW_SCHEMA)


def _failure_reasons(spark, **overrides):
    df = tag_dq_failures(_row(spark, **overrides))
    return df.collect()[0]["_dq_failure_reasons"]


def test_a_fully_valid_row_has_no_failure_reasons(spark):
    assert _failure_reasons(spark) == []


def test_multilingual_titles_never_fail_validity(spark):
    for title in ("مقالة عن ويكيبيديا", "北京市历史", "Cool page 🎉 update"):
        assert _failure_reasons(spark, title=title) == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"_event_id": None},
        {"title": None},
        {"wiki": None},
        {"meta_dt": None},
    ],
)
def test_completeness_fails_on_missing_required_field(spark, overrides):
    assert _failure_reasons(spark, **overrides) == ["completeness"]


def test_validity_fails_on_unknown_type(spark):
    assert _failure_reasons(spark, type="unknown_type") == ["validity"]


def test_validity_fails_on_control_character_in_title(spark):
    assert _failure_reasons(spark, title="Bad\x01title") == ["validity"]


def test_accuracy_fails_when_new_type_has_nonzero_length_old(spark):
    assert _failure_reasons(spark, type="new", length_old=50) == ["accuracy"]


def test_accuracy_passes_when_new_type_has_null_or_zero_length_old(spark):
    assert _failure_reasons(spark, type="new", length_old=None) == []
    assert _failure_reasons(spark, type="new", length_old=0) == []


def test_accuracy_fails_beyond_clock_skew_tolerance(spark):
    far_future = datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert _failure_reasons(
        spark, timestamp=int(far_future.timestamp()), dt=far_future.strftime("%Y-%m-%d")
    ) == ["accuracy"]


def test_consistency_fails_when_partition_date_does_not_match_timestamp(spark):
    assert _failure_reasons(spark, dt="2020-01-01") == ["consistency"]


def test_multiple_dimensions_can_fail_at_once(spark):
    reasons = _failure_reasons(spark, title=None, dt="2020-01-01")
    assert set(reasons) == {"completeness", "consistency"}


def test_timeliness_flags_late_arrival_without_blocking(spark):
    late_ingested_at = "2026-08-01T10:20:00+00:00"  # 20 min after meta_dt, past the 600s watermark
    df = tag_timeliness(_row(spark, _ingested_at=late_ingested_at))
    row = df.collect()[0]
    assert row["_ingestion_latency_seconds"] > TIMELINESS_WATERMARK_SECONDS
    assert row["_is_late_arrival"] is True


def test_timeliness_does_not_flag_a_prompt_arrival(spark):
    df = tag_timeliness(_row(spark))
    row = df.collect()[0]
    assert row["_is_late_arrival"] is False
