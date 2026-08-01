"""Generates local, machine-specific Spark configuration from .env.

Neither generated file is committed (see .gitignore) -- mirrors how `.env`
itself is generated from `.env.example` (convention 9.8). Produces:

- pipelines/spark-pipeline.yml, from pipelines/spark-pipeline.yml.example:
  fills in the absolute pipeline storage root, and (for dim_wiki_reference's
  SQL variant only) the raw snapshot path -- see
  docs/SPEC-phase2-bronze.md Section 4.3/7.3 for why this can't just be an
  OS environment variable substituted automatically.
- local-stack/.spark-conf/spark-defaults.conf: a persistent local Hive
  metastore plus the Hadoop S3A/GCS connector jars, both of which must be in
  place *before* the Spark JVM boots -- "static" Spark configs that
  spark-pipeline.yml's own `configuration:` block can't set after the fact
  (Section 4.3).

Run once, and again after any relevant `.env` change:
    python scripts/render_local_spark_config.py
Then, before any spark-pipelines command:
    export SPARK_CONF_DIR="$(pwd)/local-stack/.spark-conf"

Requires the local stack (or real cloud credentials) already reachable,
since resolving the dim_wiki_reference path goes through the same
StorageBackend Phase 1 uses (e.g. `docker compose -f
local-stack/docker-compose.yml up -d` first, for the local MinIO default).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

from src.shared.backend_factory import get_storage_backend

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_CONF_DIR = REPO_ROOT / "local-stack" / ".spark-conf"
HADOOP_AWS_VERSION = "3.4.2"  # matches the hadoop-client version pyspark[pipelines] bundles
GCS_CONNECTOR_COORDINATE = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.23"
# Matches pyspark[pipelines]'s Spark/Scala build (delta-spark's artifact id is
# Spark-version-qualified from Delta 4.x onward) and the delta-spark version
# `uv add` resolved into pyproject.toml.
DELTA_PACKAGE_COORDINATE = "io.delta:delta-spark_4.1_2.13:4.3.1"


def _hive_metastore_lines(conf_dir: Path) -> list[str]:
    # No javax.jdo.option.ConnectionURL here: spark-defaults.conf's loader
    # only accepts "spark."-prefixed keys and silently drops anything else
    # with a one-line "Ignoring non-Spark config property" warning -- easy
    # to miss, verified empirically. The embedded Derby metastore's own
    # location (metastore_db/, derby.log) therefore can't be redirected this
    # way; it always lands relative to whatever directory spark-pipelines is
    # invoked from (both gitignored at the repo root -- see docs/RUNBOOK.md,
    # which is why every spark-pipelines command runs from the repo root).
    # spark.sql.warehouse.dir *is* "spark."-prefixed and works normally.
    warehouse_dir = conf_dir / "warehouse"
    return [
        "spark.sql.catalogImplementation hive",
        f"spark.sql.warehouse.dir file://{warehouse_dir}",
    ]


def _delta_lines() -> list[str]:
    # Delta needs its own SparkSession extension + catalog registered, or
    # @dp.table's Delta-specific table_properties (e.g.
    # delta.enableChangeDataFeed) are silently no-ops and the table
    # materializes as plain Parquet instead -- verified empirically.
    return [
        "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # Without this, the Hive catalog entry SDP registers before the
        # streaming write starts can default to a different provider than
        # Delta expects, and a second separate run fails with
        # DELTA_CANNOT_CHANGE_PROVIDER -- verified empirically.
        "spark.sql.sources.default delta",
    ]


def _s3_compatible_lines(jars_packages: list[str]) -> list[str]:
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    jars_packages.append(f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}")
    lines = [
        f"spark.hadoop.fs.s3a.access.key {os.environ['AWS_ACCESS_KEY_ID']}",
        f"spark.hadoop.fs.s3a.secret.key {os.environ['AWS_SECRET_ACCESS_KEY']}",
        "spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem",
    ]
    if endpoint:  # minio (or another self-hosted S3-compatible store): custom endpoint, path-style access
        lines += [
            f"spark.hadoop.fs.s3a.endpoint {endpoint}",
            "spark.hadoop.fs.s3a.path.style.access true",
            f"spark.hadoop.fs.s3a.connection.ssl.enabled {str(endpoint.startswith('https')).lower()}",
        ]
    else:  # real AWS S3: no custom endpoint, region required instead
        lines.append(f"spark.hadoop.fs.s3a.endpoint.region {os.environ['AWS_REGION']}")
    return lines


def _gcs_lines(jars_packages: list[str]) -> list[str]:
    jars_packages.append(GCS_CONNECTOR_COORDINATE)
    return [
        "spark.hadoop.fs.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        f"spark.hadoop.google.cloud.auth.service.account.json.keyfile {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}",
    ]


def _render_spark_defaults_conf(conf_dir: Path = SPARK_CONF_DIR) -> Path:
    backend = os.environ["STORAGE_BACKEND"]  # minio | s3 | gcs
    # spark.jars.packages can only be set once (a later line would overwrite,
    # not merge, with an earlier one) -- built up as one comma-separated list.
    jars_packages = [DELTA_PACKAGE_COORDINATE]
    connector_lines = _gcs_lines(jars_packages) if backend == "gcs" else _s3_compatible_lines(jars_packages)
    lines = (
        [f"spark.jars.packages {','.join(jars_packages)}"]
        + _hive_metastore_lines(conf_dir)
        + _delta_lines()
        + connector_lines
    )

    conf_dir.mkdir(parents=True, exist_ok=True)
    out_path = conf_dir / "spark-defaults.conf"
    out_path.write_text("\n".join(lines) + "\n")
    return out_path


def _render_pipeline_spec(
    out_path: Path = REPO_ROOT / "pipelines" / "spark-pipeline.yml",
    storage_root: Path = REPO_ROOT / "pipelines" / ".pipeline-storage",
) -> Path:
    template_path = REPO_ROOT / "pipelines" / "spark-pipeline.yml.example"
    raw_dim_wiki_reference_path = get_storage_backend().resolve_uri("dim_wiki_reference")

    rendered = template_path.read_text().replace(
        "__PIPELINE_STORAGE_ROOT__", str(storage_root)
    ).replace("__RAW_DIM_WIKI_REFERENCE_PATH__", raw_dim_wiki_reference_path)

    out_path.write_text(rendered)
    return out_path


def main() -> None:
    load_dotenv()
    spark_defaults_path = _render_spark_defaults_conf()
    pipeline_spec_path = _render_pipeline_spec()
    print(f"Wrote {spark_defaults_path}")
    print(f"Wrote {pipeline_spec_path}")
    print(f'Next: export SPARK_CONF_DIR="{SPARK_CONF_DIR}"')


if __name__ == "__main__":
    main()
