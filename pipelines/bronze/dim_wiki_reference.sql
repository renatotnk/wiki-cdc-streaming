-- bronze_dim_wiki_reference SQL variant. Named bronze_dim_wiki_reference_sql,
-- not bronze_dim_wiki_reference: this and dim_wiki_reference.py both declare
-- a table for the same dataset (convention 9.5.1 -- generate both, pick
-- one), but spark-pipelines rejects two flows targeting the same output
-- name in one dataflow graph ([PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT],
-- verified empirically). Once you've picked a variant, rename its table
-- back to bronze_dim_wiki_reference and delete the other file.
--
-- Uses json.`path` (standard Spark SQL batch source) instead of
-- read_files, which doesn't exist in open-source Spark (SPEC-phase2-
-- bronze.md Section 4.2) -- explicit CASTs stand in for an explicit read
-- schema, since json.`path` can't take one directly.
--
-- ${spark.wikicdc.raw_dim_wiki_reference_path} is a literal value in
-- spark-pipeline.yml's configuration: block, not auto-populated from .env
-- (Section 4.3/7.3): a SQL file can't call get_storage_backend(), and
-- spark.conf.set(...) is blocked inside pipeline registration, so there's
-- no channel to pass a dynamically-resolved value from Python into a
-- sibling SQL file. Edit that value if BUCKET_NAME/STORAGE_BACKEND differ
-- from the committed local-MinIO default.
CREATE MATERIALIZED VIEW bronze_dim_wiki_reference_sql AS
SELECT
  CAST(wiki_code AS STRING) AS wiki_code,
  CAST(language_name AS STRING) AS language_name,
  CAST(project_type AS STRING) AS project_type,
  CAST(is_closed AS BOOLEAN) AS is_closed,
  CAST(_snapshot_fetched_at AS TIMESTAMP) AS _snapshot_fetched_at
FROM json.`${spark.wikicdc.raw_dim_wiki_reference_path}`;
