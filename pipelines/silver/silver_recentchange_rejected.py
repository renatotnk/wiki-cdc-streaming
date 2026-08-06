"""silver_recentchange_rejected -- the P4 reconciliation counterpart to
silver_recentchange.py: rows that failed at least one of the 4 traceable
blocking DQ dimensions, with `_dq_failure_reasons` naming which one(s)
(SPEC-phase3-silver-cdf.md Section 8.3/11 acceptance criterion 2). Not
populated by reading an SDP "expectation event log" -- that mechanism
doesn't exist (pipelines/silver/dq_rules.py's module docstring) -- populated
by the complementary filter on the same `_dq_failure_reasons` column
silver_recentchange.py filters on, computed once upstream in
silver_recentchange_staging.py.

No contract file for this table (contracts/silver_recentchange.contract.yaml
covers silver_recentchange only) -- nothing downstream consumes
_rejected's shape, so a formal P8 contract isn't justified here (YAGNI).

Like silver_recentchange.py, this table can only be added to the pipeline
spec (and run) *after* silver_recentchange_staging_py has already been
materialized by an earlier run -- see silver_recentchange_staging.py's
docstring for the full explanation.
"""

from dotenv import load_dotenv
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import size

load_dotenv()

spark = SparkSession.active()


@dp.table(
    name="silver_recentchange_rejected_py",  # rename to silver_recentchange_rejected once chosen (convention 9.5.1)
    partition_cols=["dt", "hour"],
)
def silver_recentchange_rejected_py():
    staging = spark.readStream.format("delta").table("silver_recentchange_staging_py")
    return staging.filter(size("_dq_failure_reasons") > 0)
