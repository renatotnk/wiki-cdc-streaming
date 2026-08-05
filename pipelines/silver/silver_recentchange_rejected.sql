-- silver_recentchange_rejected_sql -- SQL variant of
-- silver_recentchange_rejected.py: the complementary filter on the same
-- staging table, same caveats (see silver_recentchange_staging.sql).
CREATE STREAMING TABLE silver_recentchange_rejected_sql
AS SELECT *
FROM STREAM silver_recentchange_staging_sql
WHERE size(_dq_failure_reasons) > 0;
