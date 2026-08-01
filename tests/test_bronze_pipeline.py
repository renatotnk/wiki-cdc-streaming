"""bronze_recentchange: spark-pipelines dry-run + a real local execution
against a raw fixture, validating row counts (SPEC-phase2-bronze.md
Section 12, acceptance criteria 1 and 2).

Requires the local stack (`docker compose -f local-stack/docker-compose.yml
up -d`) -- skipped, not failed, if MinIO isn't reachable (mirrors
tests/test_ingestion_smoke.py). Uses its own throwaway Spark config
(SPARK_CONF_DIR/pipeline spec pointed at pytest's tmp_path, built with the
same helpers as scripts/render_local_spark_config.py) so it never touches a
developer's real local metastore/warehouse, and filters by a unique
_event_id prefix per test run so it tolerates whatever else already exists
under raw/ in the bucket (same technique as test_ingestion_smoke.py's
run_marker).

Known flaky failure: the spark-pipelines subprocess occasionally exits with
`ModuleNotFoundError: No module named 'pipelines'` despite an explicit,
absolute PYTHONPATH being passed in its env -- observed only when this file
runs alongside other tests in the same pytest session, never in isolation,
and not reproducible via the identical subprocess.run() call made directly
outside pytest. Root cause not found; _run_spark_pipelines_with_retry works
around it with a bounded retry rather than blocking on a full diagnosis.
"""

import os
import subprocess
import uuid
from pathlib import Path

import polars as pl
import pytest

import scripts.render_local_spark_config as render_config
from src.handlers.storage.s3_compatible_storage_handler import S3CompatibleStorageHandler
from src.shared.backend_factory import get_storage_backend

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_PIPELINES_TIMEOUT_SECONDS = 300


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
    """Isolated spark-defaults.conf + spark-pipeline.yml, built with the same
    helpers scripts/render_local_spark_config.py uses -- pointed at tmp_path
    so this test never touches the developer's real local Spark config.

    The spec file itself must live inside the real pipelines/ directory
    (its `bronze/**` library glob is resolved relative to its own parent
    directory, not cwd -- SPEC-phase2-bronze.md Section 8), but the
    subprocess's cwd is tmp_path, so Derby's own metastore_db/derby.log
    (which can't be redirected via spark-defaults.conf -- Section 4.3) land
    there instead of polluting a developer's real repo-root metastore.
    """
    spark_conf_dir = tmp_path / "spark-conf"
    render_config._render_spark_defaults_conf(spark_conf_dir)

    spec_path = REPO_ROOT / "pipelines" / "spark-pipeline.test.yml"
    render_config._render_pipeline_spec(out_path=spec_path, storage_root=tmp_path / "pipeline-storage")
    try:
        yield spec_path, spark_conf_dir, tmp_path
    finally:
        spec_path.unlink(missing_ok=True)


def _write_raw_fixture(event_ids: list[str], hour: str) -> None:
    handler = S3CompatibleStorageHandler()
    # Every field gets a real value, not None -- an all-None column in a
    # single write_parquet() call gets a polars-inferred type (e.g. Int32)
    # that can conflict with BRONZE_RECENTCHANGE_SCHEMA's declared type and
    # fail the whole batch with a PARQUET_COLUMN_DATA_TYPE_MISMATCH.
    meta = {
        "dt": f"2026-08-01T{hour}:00:00Z", "uri": "https://en.wikipedia.org/w/index.php",
        "request_id": "req-1", "id": "uuid-1", "domain": "en.wikipedia.org",
        "stream": "recentchange", "topic": "eqiad.mediawiki.recentchange",
        "partition": 0, "offset": 1,
    }
    df = pl.DataFrame(
        {
            "id": list(range(len(event_ids))),
            "type": ["edit"] * len(event_ids),
            "title": [f"test page {e}" for e in event_ids],
            "user": ["test-user"] * len(event_ids),
            "bot": [False] * len(event_ids),
            "wiki": ["enwiki"] * len(event_ids),
            "timestamp": [1_700_000_000] * len(event_ids),
            "server_url": ["https://en.wikipedia.org"] * len(event_ids),
            "meta": [meta] * len(event_ids),
            "length": [{"old": 1, "new": 2}] * len(event_ids),
            "_event_id": event_ids,
            "_ingested_at": ["2026-08-01T10:00:01Z"] * len(event_ids),
            "_producer_instance": ["pytest"] * len(event_ids),
            "_extra_fields": ["{}"] * len(event_ids),
            "_schema_version": ["1.0.0"] * len(event_ids),
        }
    )
    handler.write(df, f"raw/dt=2026-08-01/hour={hour}/part-{uuid.uuid4()}.parquet", format="parquet")


SPARK_PIPELINES_BIN = REPO_ROOT / ".venv" / "bin" / "spark-pipelines"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


def _run_spark_pipelines(
    command: str, spec_path: Path, spark_conf_dir: Path, subprocess_cwd: Path
) -> subprocess.CompletedProcess:
    # Invoke the venv's spark-pipelines script directly, not `uv run
    # spark-pipelines`: `uv run` resolves the project from cwd upward, which
    # breaks once cwd is subprocess_cwd (tmp_path, outside the repo).
    # PYSPARK_PYTHON pins the script's own `find-spark-home` resolution to
    # this venv instead of falling back to a bare `python3` on PATH.
    # PYTHONPATH must be absolute, not "." -- cwd is subprocess_cwd, not the
    # repo root, so a relative "." would break `from pipelines.bronze...` /
    # `from src.shared...` imports inside the pipeline definition files.
    env = os.environ | {
        "PYTHONPATH": str(REPO_ROOT),
        "SPARK_CONF_DIR": str(spark_conf_dir),
        "PYSPARK_PYTHON": str(VENV_PYTHON),
    }
    return subprocess.run(
        [str(SPARK_PIPELINES_BIN), command, "--spec", str(spec_path)],
        cwd=subprocess_cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=SPARK_PIPELINES_TIMEOUT_SECONDS,
    )


SPARK_PIPELINES_MAX_ATTEMPTS = 2  # mitigates the known flake described in the module docstring


def _run_spark_pipelines_with_retry(
    command: str, spec_path: Path, spark_conf_dir: Path, subprocess_cwd: Path
) -> subprocess.CompletedProcess:
    result = _run_spark_pipelines(command, spec_path, spark_conf_dir, subprocess_cwd)
    for _ in range(SPARK_PIPELINES_MAX_ATTEMPTS - 1):
        if result.returncode == 0:
            break
        result = _run_spark_pipelines(command, spec_path, spark_conf_dir, subprocess_cwd)
    return result


def _read_bronze_rows(spark_conf_dir: Path) -> pl.DataFrame:
    files = list((spark_conf_dir / "warehouse" / "bronze_recentchange").glob("dt=*/hour=*/*.parquet"))
    if not files:
        return pl.DataFrame()
    return pl.read_parquet(files)


def test_dry_run_validates_the_pipeline_without_touching_data(test_pipeline_spec):
    spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_spec

    result = _run_spark_pipelines_with_retry("dry-run", spec_path, spark_conf_dir, subprocess_cwd)

    assert result.returncode == 0, result.stdout + result.stderr


def test_real_execution_matches_raw_row_count_and_reruns_incrementally(test_pipeline_spec):
    spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_spec
    marker = uuid.uuid4().hex[:8]
    first_batch = [f"{marker}-1", f"{marker}-2", f"{marker}-3"]
    _write_raw_fixture(first_batch, hour="10")

    first_run = _run_spark_pipelines_with_retry("run", spec_path, spark_conf_dir, subprocess_cwd)
    assert first_run.returncode == 0, first_run.stdout + first_run.stderr

    rows = _read_bronze_rows(spark_conf_dir)
    marker_rows = rows.filter(pl.col("_event_id").str.starts_with(marker))
    assert marker_rows.height == len(first_batch)  # criterion 1: consolidation without loss

    rerun_with_no_new_files = _run_spark_pipelines_with_retry("run", spec_path, spark_conf_dir, subprocess_cwd)
    assert rerun_with_no_new_files.returncode == 0
    rows = _read_bronze_rows(spark_conf_dir)
    marker_rows = rows.filter(pl.col("_event_id").str.starts_with(marker))
    assert marker_rows.height == len(first_batch)  # no duplicates from reprocessing

    second_batch = [f"{marker}-4"]
    _write_raw_fixture(second_batch, hour="11")
    second_run = _run_spark_pipelines_with_retry("run", spec_path, spark_conf_dir, subprocess_cwd)
    assert second_run.returncode == 0, second_run.stdout + second_run.stderr

    rows = _read_bronze_rows(spark_conf_dir)
    marker_rows = rows.filter(pl.col("_event_id").str.starts_with(marker))
    assert marker_rows.height == len(first_batch) + len(second_batch)  # criterion 2: automatic incrementality
    assert set(marker_rows["_event_id"]) == set(first_batch + second_batch)
