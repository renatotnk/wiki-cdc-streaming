"""Explicit Spark schemas for bronze datasets.

Source of truth is contracts/bronze_recentchange.contract.yaml and
contracts/dim_wiki_reference.contract.yaml -- this module is just their
Spark StructType encoding, imported by bronze_recentchange.py and
dim_wiki_reference.py (dim_wiki_reference.sql encodes its own copy via
explicit CASTs instead, since a standalone SQL file can't import Python --
see SPEC-phase2-bronze.md Section 7.3).
"""

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_META_SCHEMA = StructType(
    [
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", LongType(), True),
        StructField("offset", LongType(), True),
    ]
)

_LENGTH_SCHEMA = StructType(
    [
        StructField("old", LongType(), True),
        StructField("new", LongType(), True),
    ]
)

BRONZE_RECENTCHANGE_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("user", StringType(), True),
        StructField("bot", BooleanType(), True),
        StructField("wiki", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("server_url", StringType(), True),
        StructField("meta", _META_SCHEMA, True),
        StructField("length", _LENGTH_SCHEMA, True),
        StructField("_event_id", StringType(), True),
        # StringType, not TimestampType: src/producer/main.py writes
        # datetime.now(timezone.utc).isoformat() -- an ISO8601 string, not a
        # native timestamp -- despite SPEC-phase1-ingestion.md Section 6's
        # prose table saying "timestamp". Bronze mirrors what Phase 1
        # actually produces (P8 is about the contracted shape of real data,
        # not the aspirational shape from the earlier SPEC's prose).
        StructField("_ingested_at", StringType(), True),
        StructField("_producer_instance", StringType(), True),
        StructField("_extra_fields", StringType(), True),
        StructField("_schema_version", StringType(), True),
    ]
)

DIM_WIKI_REFERENCE_SCHEMA = StructType(
    [
        StructField("wiki_code", StringType(), False),
        StructField("language_name", StringType(), True),
        StructField("project_type", StringType(), True),
        StructField("is_closed", BooleanType(), True),
        StructField("_snapshot_fetched_at", TimestampType(), True),
    ]
)
