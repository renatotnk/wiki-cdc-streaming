"""bronze_dim_wiki_reference: a new snapshot file is picked up without
reprocessing/duplicating prior snapshots (SPEC-phase2-bronze.md Section 12),
and the Python/SQL variants stay equivalent (acceptance criterion 5).

Requires the local stack (`docker compose -f local-stack/docker-compose.yml
up -d`) -- skipped, not failed, if MinIO isn't reachable. Uses synthetic
snapshot fixtures (not the real Wikimedia API -- FIRST/Isolated) written
directly to dim_wiki_reference/, and its own throwaway Spark config
(tmp_path), same as tests/test_bronze_pipeline.py.

Verifies row counts via DuckDB's delta_scan(), not by globbing the
warehouse directory's Parquet files: bronze_dim_wiki_reference is a
materialized view, refreshed via TRUNCATE + rewrite on every run -- the old
run's files remain on disk as Delta tombstones, so only a
transaction-log-aware reader reports the table's true current content
(this tripped up manual verification during implementation; see
scripts/inspect_bronze.py's docstring).
"""

import os
import subprocess
import uuid
from pathlib import Path

import duckdb
import polars as pl
import pytest

import scripts.render_local_spark_config as render_config
from src.handlers.storage.s3_compatible_storage_handler import S3CompatibleStorageHandler
from src.shared.backend_factory import get_storage_backend

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_PIPELINES_BIN = REPO_ROOT / ".venv" / "bin" / "spark-pipelines"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"
SPARK_PIPELINES_TIMEOUT_SECONDS = 300
TABLE_NAMES = ["bronze_dim_wiki_reference_py", "bronze_dim_wiki_reference_sql"]


def _local_stack_available() -> bool:
    try:
        get_storage_backend()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _local_stack_available(),
    reason="local stack (MinIO) is not reachable -- run "
    "`docker compose -f local-stack/docker-compose.yml up -d` first",
)


@pytest.fixture
def test_pipeline_spec(tmp_path):
    """Isolated spark-defaults.conf + spark-pipeline.yml -- see
    tests/test_bronze_pipeline.py's identical fixture for why cwd is
    tmp_path, the spec file still lives inside the real pipelines/ dir, and
    this renders the bronze-only spec rather than the full one.
    """
    spark_conf_dir = tmp_path / "spark-conf"
    render_config._render_spark_defaults_conf(spark_conf_dir)

    spec_path = REPO_ROOT / "pipelines" / "spark-pipeline.test.yml"
    render_config._render_bronze_only_pipeline_spec(out_path=spec_path, storage_root=tmp_path / "pipeline-storage")
    try:
        yield spec_path, spark_conf_dir, tmp_path
    finally:
        spec_path.unlink(missing_ok=True)


def _write_snapshot_fixture(marker: str, wiki_codes: list[str]) -> str:
    handler = S3CompatibleStorageHandler()
    df = pl.DataFrame(
        {
            "wiki_code": [f"{marker}-{code}" for code in wiki_codes],
            "language_name": ["Test Language"] * len(wiki_codes),
            "project_type": ["wikipedia"] * len(wiki_codes),
            "is_closed": [False] * len(wiki_codes),
            "_snapshot_fetched_at": ["2026-08-01T10:00:00Z"] * len(wiki_codes),
        }
    )
    path = f"dim_wiki_reference/snapshot-{uuid.uuid4()}.json"
    handler.write(df, path, format="ndjson")
    return path


def _run_spark_pipelines(spec_path: Path, spark_conf_dir: Path, subprocess_cwd: Path) -> subprocess.CompletedProcess:
    env = os.environ | {
        "PYTHONPATH": str(REPO_ROOT),
        "SPARK_CONF_DIR": str(spark_conf_dir),
        "PYSPARK_PYTHON": str(VENV_PYTHON),
    }
    return subprocess.run(
        [str(SPARK_PIPELINES_BIN), "run", "--spec", str(spec_path)],
        cwd=subprocess_cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=SPARK_PIPELINES_TIMEOUT_SECONDS,
    )


def _marker_row_count(spark_conf_dir: Path, table_name: str, marker: str) -> int:
    table_dir = spark_conf_dir / "warehouse" / table_name
    con = duckdb.connect()
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    query = f"SELECT COUNT(*) FROM delta_scan('{table_dir}') WHERE wiki_code LIKE '{marker}-%'"
    return con.execute(query).fetchone()[0]


def test_new_snapshot_is_picked_up_without_duplicating_prior_ones(test_pipeline_spec):
    spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_spec
    marker = uuid.uuid4().hex[:8]
    _write_snapshot_fixture(marker, ["a", "b", "c"])

    first_run = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    assert first_run.returncode == 0, first_run.stdout + first_run.stderr
    for table_name in TABLE_NAMES:
        assert _marker_row_count(spark_conf_dir, table_name, marker) == 3

    rerun_with_no_new_snapshot = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    assert rerun_with_no_new_snapshot.returncode == 0
    for table_name in TABLE_NAMES:
        assert _marker_row_count(spark_conf_dir, table_name, marker) == 3  # not duplicated to 6

    _write_snapshot_fixture(marker, ["d"])
    second_run = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    assert second_run.returncode == 0, second_run.stdout + second_run.stderr
    for table_name in TABLE_NAMES:
        assert _marker_row_count(spark_conf_dir, table_name, marker) == 4  # criterion 3: new snapshot reflected


def test_python_and_sql_variants_produce_identical_content(test_pipeline_spec):
    spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_spec
    marker = uuid.uuid4().hex[:8]
    _write_snapshot_fixture(marker, ["x", "y"])

    run = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    assert run.returncode == 0, run.stdout + run.stderr

    con = duckdb.connect()
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    rows_by_table = {}
    for table_name in TABLE_NAMES:
        table_dir = spark_conf_dir / "warehouse" / table_name
        query = f"SELECT * FROM delta_scan('{table_dir}') WHERE wiki_code LIKE '{marker}-%' ORDER BY wiki_code"
        rows_by_table[table_name] = con.execute(query).df().drop(columns=["_snapshot_fetched_at"])

    py_rows, sql_rows = rows_by_table.values()
    assert py_rows.equals(sql_rows)  # criterion 5: language-variant equivalence
