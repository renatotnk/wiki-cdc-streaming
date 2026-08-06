-- silver_recentchange_staging_sql -- SQL variant of silver_recentchange_staging.py.
-- See that file's docstring for the shared architecture rationale (why a
-- staging table exists, why SDP expectations aren't used, why bronze must
-- already be materialized by an earlier, separate run before this file can
-- resolve `bronze_recentchange`).
--
-- `CREATE STREAMING TABLE`, not `CREATE OR REFRESH STREAMING TABLE` (the
-- original SPEC draft's syntax) -- verified empirically: "OR REFRESH" only
-- parses for `CREATE MATERIALIZED VIEW` (see dim_wiki_reference.sql),
-- `CREATE OR REFRESH STREAMING TABLE` throws a bare ParseException.
--
-- `FROM STREAM bronze_recentchange`, not `.option("readChangeFeed", "true")`:
-- no SQL clause for that option exists in this SDP grammar (verified
-- empirically) -- this variant only ever sees a plain incremental read of
-- bronze, which happens to equal its CDF today (bronze is append-only) but
-- would silently miss a future MERGE/UPDATE/DELETE correction. Only the
-- Python variant can express the CDF-specific read (SPEC-phase3-silver-cdf.md
-- Section 4's own callout).
--
-- No normalize_nfc(...) here, unlike the Python variant: verified
-- empirically that registering a Python UDF from *inside* a pipeline file
-- -- even at plain module level, not inside a query function -- throws
-- `[SESSION_MUTATION_IN_DECLARATIVE_PIPELINE.REGISTER_UDF]`. SDP blocks
-- session mutations (including UDF registration) from any file it imports,
-- with no documented escape hatch found -- title/user are therefore not
-- NFC-normalized in this variant at all (a real, disclosed correctness gap,
-- not a stylistic omission: an NFC/NFD duplicate pair would be treated as
-- distinct here).
--
-- Known gap versus the Python variant: no streaming-safe dedup here either.
-- `dropDuplicatesWithinWatermark` (used in silver_recentchange_staging.py)
-- has no discoverable SQL-clause equivalent in this SDP grammar (`WATERMARK
-- ... DELAY OF INTERVAL ...` parses, but no paired "drop duplicates within
-- watermark" clause was found) -- the uniqueness dimension is therefore not
-- enforced at all in this variant, on top of the disclosed limitation
-- already accepted for the Python variant (duplicates not traceable to
-- rejected either way). Between this and the UDF restriction above, this
-- variant is a real illustration of convention 9.5.1's own framing: SQL
-- variants aren't always full peers of the Python one, which is exactly why
-- the project keeps both only long enough to compare, then deletes one,
-- rather than maintaining two indefinitely.
CREATE STREAMING TABLE silver_recentchange_staging_sql
AS SELECT
  *,
  filter(
    array(
      CASE 
        WHEN NOT (
          _event_id IS NOT NULL 
          AND title IS NOT NULL 
          AND wiki IS NOT NULL 
          AND meta_dt IS NOT NULL
        ) THEN 'completeness' 
      END,
      CASE 
        WHEN NOT (
          type IN ('edit', 'new', 'log', 'categorize')
          AND NOT rlike(title, concat('[', chr(0), '-', chr(8), chr(11), chr(12), chr(14), '-', chr(31), ']'))
        ) THEN 'validity' 
      END,
      CASE 
        WHEN NOT (
          (
            type != 'new' OR length_old IS NULL OR length_old = 0)
            AND timestamp <= unix_timestamp(current_timestamp()) + ${spark.wikicdc.clock_skew_tolerance_seconds}
        ) THEN 'accuracy' 
      END,
      CASE 
        WHEN NOT (
          dt = date_format(from_unixtime(timestamp), 'yyyy-MM-dd')
        ) THEN 'consistency' 
      END
  ), x -> x IS NOT NULL) AS _dq_failure_reasons,
  unix_timestamp(to_timestamp(_ingested_at)) - unix_timestamp(meta_dt) AS _ingestion_latency_seconds,
  (unix_timestamp(to_timestamp(_ingested_at)) - unix_timestamp(meta_dt)) > ${spark.wikicdc.timeliness_watermark_seconds} AS _is_late_arrival,
  current_timestamp() AS _silver_loaded_at
FROM (
  SELECT
    _event_id, 
    type, 
    title, 
    user, 
    bot, 
    wiki, 
    timestamp,
    server_url, 
    CAST(meta.dt AS TIMESTAMP) AS meta_dt, 
    length.old AS length_old, 
    length.new AS length_new,
    _ingested_at, 
    _producer_instance, 
    _extra_fields, 
    _schema_version, 
    dt, 
    hour
  FROM STREAM bronze_recentchange
);
