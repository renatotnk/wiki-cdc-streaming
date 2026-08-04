"""bronze_recentchange -- SDP streaming table over Phase 1's raw Parquet
output. Python-only: declaring a streaming source over an arbitrary file
path needs `spark.readStream(...).load(path)`, which open-source Spark's
SQL can't express (no `read_files`/`STREAM <path>` there -- see
docs/SPEC-phase2-bronze.md Section 4.2). Applies typing only, no row-level
validation (Section 4.1 of the same doc) -- Phase 3 owns that.
"""

from dotenv import load_dotenv
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from pipelines.bronze.schema import BRONZE_RECENTCHANGE_SCHEMA
from src.shared.backend_factory import get_storage_backend

# Unconditional, not inside `if __name__ == "__main__":` -- unlike
# producer/consumer (always run directly), spark-pipelines *imports* this
# file as a regular module, so a __main__-gated load_dotenv() never fires.
load_dotenv()

spark = SparkSession.active()
raw_recentchange_path = get_storage_backend().resolve_uri("raw")


@dp.table(
    name="bronze_recentchange",
    # No explicit format="delta" here -- verified empirically that passing it
    # explicitly triggers a broken reconciliation path on any second, separate
    # `spark-pipelines run` invocation ([DELTA_CANNOT_CHANGE_PROVIDER]).
    # spark.sql.sources.default=delta (local-stack/.spark-conf/spark-defaults.conf,
    # scripts/render_local_spark_config.py) makes this a genuine Delta table
    # without hitting that path.
    table_properties={"delta.enableChangeDataFeed": "true"},
    partition_cols=["dt", "hour"],
)
def bronze_recentchange():
    return (
        spark.readStream.format("parquet")
        .schema(BRONZE_RECENTCHANGE_SCHEMA)
        .load(raw_recentchange_path)
        .withColumn("_bronze_loaded_at", current_timestamp())
    )
