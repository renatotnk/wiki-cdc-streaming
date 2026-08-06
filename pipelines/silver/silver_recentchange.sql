-- silver_recentchange_sql -- SQL variant of silver_recentchange.py. Reads
-- silver_recentchange_staging_sql (must already be materialized by an
-- earlier, separate run -- see silver_recentchange_staging.sql's comment)
-- and keeps only rows that failed none of the 4 traceable blocking
-- dimensions.
CREATE STREAMING TABLE silver_recentchange_sql
AS SELECT
  _event_id, 
  type, 
  title, 
  user, 
  bot, 
  wiki, 
  timestamp, 
  server_url, 
  meta_dt, 
  length_old, 
  length_new,
  _ingested_at, 
  _producer_instance, 
  _extra_fields, 
  _schema_version, 
  _ingestion_latency_seconds,
  _is_late_arrival, 
  _silver_loaded_at, 
  dt, 
  hour
FROM STREAM silver_recentchange_staging_sql
WHERE size(_dq_failure_reasons) = 0;
