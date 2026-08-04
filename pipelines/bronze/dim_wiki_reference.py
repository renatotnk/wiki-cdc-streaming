"""bronze_dim_wiki_reference -- SDP batch materialized view over
scripts/fetch_wiki_sitematrix.py's NDJSON snapshots. Batch, not streaming:
new snapshots arrive irregularly/manually, not continuously (SPEC-phase2-
bronze.md Section 3.1).

Named bronze_dim_wiki_reference_py, not bronze_dim_wiki_reference: this and
dim_wiki_reference.sql both declare a table for the same dataset (convention
9.5.1 -- generate both, pick one), but spark-pipelines rejects two flows
targeting the same output name in one dataflow graph
([PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT], verified empirically). Once you've
picked a variant, rename its table back to bronze_dim_wiki_reference and
delete the other file.
"""

from dotenv import load_dotenv
from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from pipelines.bronze.schema import DIM_WIKI_REFERENCE_SCHEMA
from src.shared.backend_factory import get_storage_backend

# Unconditional, not inside `if __name__ == "__main__":` -- unlike
# producer/consumer (always run directly), spark-pipelines *imports* this
# file as a regular module, so a __main__-gated load_dotenv() never fires.
load_dotenv()

spark = SparkSession.active()
raw_dim_wiki_reference_path = get_storage_backend().resolve_uri("dim_wiki_reference")


@dp.materialized_view(name="bronze_dim_wiki_reference_py")
def bronze_dim_wiki_reference_py():
    return spark.read.format("json").schema(DIM_WIKI_REFERENCE_SCHEMA).load(raw_dim_wiki_reference_path)
