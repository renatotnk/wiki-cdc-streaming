"""silver_recentchange / silver_recentchange_rejected: spark-pipelines
dry-run + a real local execution against a raw fixture, validating the P4
reconciliation invariant, rejection traceability, multilingual survival,
late-arrival flagging, and Python/SQL variant equivalence
(SPEC-phase3-silver-cdf.md Section 11/12).

Requires the local stack -- skipped, not failed, if MinIO isn't reachable
(mirrors tests/test_bronze_pipeline.py).

Two-phase run, not one: bronze_recentchange must already be materialized by
an earlier, *separate* `spark-pipelines run` before a spec including
silver/** is run for the first time -- verified empirically (see
pipelines/silver/silver_recentchange_staging.py's docstring). This fixture
therefore runs a bronze-only spec first, then the full bronze+silver spec --
not a single combined run.
"""

import os
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

import scripts.render_local_spark_config as render_config
from src.handlers.storage.s3_compatible_storage_handler import S3CompatibleStorageHandler
from src.shared.backend_factory import get_storage_backend

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_PIPELINES_TIMEOUT_SECONDS = 600


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
def test_pipeline_specs(tmp_path):
    """Isolated spark-defaults.conf + two spec files sharing one storage
    root: a bronze-only spec (bootstraps bronze_recentchange, see module
    docstring) and the full bronze+silver spec used for every run after
    that.
    """
    spark_conf_dir = tmp_path / "spark-conf"
    render_config._render_spark_defaults_conf(spark_conf_dir)

    full_spec_path = REPO_ROOT / "pipelines" / "spark-pipeline.silver-test.yml"
    render_config._render_pipeline_spec(out_path=full_spec_path, storage_root=tmp_path / "pipeline-storage")

    bronze_only_spec_path = REPO_ROOT / "pipelines" / "spark-pipeline.silver-test-bronze-only.yml"
    render_config._render_bronze_only_pipeline_spec(
        out_path=bronze_only_spec_path, storage_root=tmp_path / "pipeline-storage"
    )

    try:
        yield full_spec_path, bronze_only_spec_path, spark_conf_dir, tmp_path
    finally:
        full_spec_path.unlink(missing_ok=True)
        bronze_only_spec_path.unlink(missing_ok=True)


def _iso(moment) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _base_row(event_id: str, now, **overrides) -> dict:
    # meta.dt/timestamp/_ingested_at are all anchored to `now`, not a fixed
    # past date: this bucket also receives real, continuously-arriving
    # production events (the real producer/consumer never stop between test
    # runs). A hardcoded past date recedes further "into the past" every day
    # real time advances, until it eventually falls outside
    # silver_recentchange_staging.py's dedup watermark and gets silently
    # state-evicted before ever reaching the DQ tagging logic -- discovered
    # by exactly that happening to an earlier version of this fixture.
    meta = {
        "dt": _iso(now), "uri": "https://en.wikipedia.org/w/index.php",
        "request_id": "req-1", "id": "uuid-1", "domain": "en.wikipedia.org",
        "stream": "recentchange", "topic": "eqiad.mediawiki.recentchange",
        "partition": 0, "offset": 1,
    }
    row = {
        "id": 1,
        "type": "edit",
        "title": f"Test page {event_id}",
        "user": "test-user",
        "bot": False,
        "wiki": "enwiki",
        "timestamp": int(now.timestamp()),
        "server_url": "https://en.wikipedia.org",
        "meta": meta,
        "length": {"old": 1, "new": 2},
        "_event_id": event_id,
        "_ingested_at": _iso(now + timedelta(seconds=5)),
        "_producer_instance": "pytest",
        "_extra_fields": "{}",
        "_schema_version": "1.0.0",
    }
    row.update(overrides)
    return row


def _write_raw_fixture(rows: list[dict], now) -> None:
    handler = S3CompatibleStorageHandler()
    df = pl.DataFrame(rows)
    dt = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    handler.write(df, f"raw/dt={dt}/hour={hour}/part-{uuid.uuid4()}.parquet", format="parquet")


SPARK_PIPELINES_BIN = REPO_ROOT / ".venv" / "bin" / "spark-pipelines"
VENV_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


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


SPARK_PIPELINES_MAX_ATTEMPTS = 2  # mitigates the known flake documented in test_bronze_pipeline.py


def _run_spark_pipelines_with_retry(
    spec_path: Path, spark_conf_dir: Path, subprocess_cwd: Path
) -> subprocess.CompletedProcess:
    result = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    for _ in range(SPARK_PIPELINES_MAX_ATTEMPTS - 1):
        if result.returncode == 0:
            break
        result = _run_spark_pipelines(spec_path, spark_conf_dir, subprocess_cwd)
    return result


def _read_rows(spark_conf_dir: Path, table_name: str) -> pl.DataFrame:
    files = list((spark_conf_dir / "warehouse" / table_name).glob("**/*.parquet"))
    if not files:
        return pl.DataFrame()
    return pl.read_parquet(files)


def test_dry_run_validates_the_pipeline_without_touching_data(test_pipeline_specs):
    full_spec_path, bronze_only_spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_specs
    # dry-run still performs SDP's "Registering graph elements" pass across
    # every file (it just stops before "Starting execution") -- silver's
    # files fail to resolve bronze_recentchange there exactly as a real run
    # would, unless bronze already exists (see module docstring). Bootstrap
    # with a real run on the bronze-only spec first.
    bootstrap = _run_spark_pipelines_with_retry(bronze_only_spec_path, spark_conf_dir, subprocess_cwd)
    assert bootstrap.returncode == 0, bootstrap.stdout + bootstrap.stderr

    env = os.environ | {
        "PYTHONPATH": str(REPO_ROOT),
        "SPARK_CONF_DIR": str(spark_conf_dir),
        "PYSPARK_PYTHON": str(VENV_PYTHON),
    }
    result = subprocess.run(
        [str(SPARK_PIPELINES_BIN), "dry-run", "--spec", str(full_spec_path)],
        cwd=subprocess_cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=SPARK_PIPELINES_TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_real_execution_reconciles_and_tags_correctly(test_pipeline_specs):
    full_spec_path, bronze_only_spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_specs
    marker = uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)

    completeness_id = f"{marker}-completeness"
    validity_id = f"{marker}-validity"
    accuracy_id = f"{marker}-accuracy"
    consistency_id = f"{marker}-consistency"
    multilingual_id = f"{marker}-multilingual"
    duplicate_id = f"{marker}-duplicate"
    late_id = f"{marker}-late"
    valid_id = f"{marker}-valid"

    two_days_ago = now - timedelta(days=2)
    rows = [
        _base_row(valid_id, now),
        _base_row(completeness_id, now, title=None),
        _base_row(validity_id, now, title="Bad\x01title"),
        _base_row(accuracy_id, now, type="new", length={"old": 50, "new": 100}),
        # timestamp lands on a different calendar day than the dt= partition
        # (derived from `now`, Section 5's consistency dimension) while
        # meta_dt/_ingested_at stay at `now` -- fresh, watermark-safe.
        _base_row(consistency_id, now, timestamp=int(two_days_ago.timestamp())),
        _base_row(multilingual_id, now, title="مقالة عن ويكيبيديا 北京市 🎉"),
        _base_row(duplicate_id, now),
        _base_row(f"{duplicate_id}-copy", now, _event_id=duplicate_id),  # same _event_id, second copy
        # meta.dt 11 minutes before _ingested_at: exceeds the 600s (10 min)
        # timeliness threshold (dq_rules.py) while staying well within
        # staging's 2-hour dedup watermark (silver_recentchange_staging.py).
        _base_row(late_id, now - timedelta(minutes=11), _ingested_at=_iso(now)),
    ]
    _write_raw_fixture(rows, now)

    bootstrap = _run_spark_pipelines_with_retry(bronze_only_spec_path, spark_conf_dir, subprocess_cwd)
    assert bootstrap.returncode == 0, bootstrap.stdout + bootstrap.stderr

    full_run = _run_spark_pipelines_with_retry(full_spec_path, spark_conf_dir, subprocess_cwd)
    assert full_run.returncode == 0, full_run.stdout + full_run.stderr

    for suffix in ("_py", "_sql"):
        valid_rows = _read_rows(spark_conf_dir, f"silver_recentchange{suffix}")
        rejected_rows = _read_rows(spark_conf_dir, f"silver_recentchange_rejected{suffix}")

        valid_ids = set(valid_rows.filter(pl.col("_event_id").str.starts_with(marker))["_event_id"])
        rejected_by_id = {
            r["_event_id"]: r["_dq_failure_reasons"]
            for r in rejected_rows.filter(pl.col("_event_id").str.starts_with(marker)).to_dicts()
        }

        # acceptance criterion 1/2: P4 reconciliation + rejection traceability
        assert valid_id in valid_ids, suffix
        assert completeness_id in rejected_by_id and "completeness" in rejected_by_id[completeness_id], suffix
        assert validity_id in rejected_by_id and "validity" in rejected_by_id[validity_id], suffix
        assert accuracy_id in rejected_by_id and "accuracy" in rejected_by_id[accuracy_id], suffix
        assert consistency_id in rejected_by_id and "consistency" in rejected_by_id[consistency_id], suffix

        # acceptance criterion 5: multilingual content never rejected for language/script reasons
        assert multilingual_id in valid_ids, suffix
        assert multilingual_id not in rejected_by_id, suffix

        # acceptance criterion 3: timeliness flags but never blocks
        assert late_id in valid_ids, suffix
        assert late_id not in rejected_by_id, suffix
        late_row = valid_rows.filter(pl.col("_event_id") == late_id).to_dicts()[0]
        assert late_row["_is_late_arrival"] is True, suffix

        # uniqueness: the Python variant dedups (at most one surviving copy,
        # across valid+rejected combined -- disclosed limitation: which copy
        # survives, and whether it's traceable to "rejected", isn't
        # guaranteed -- see pipelines/silver/silver_recentchange_staging.py).
        # The SQL variant enforces no uniqueness dedup at all (disclosed,
        # separate gap -- same file's docstring) -- both copies survive.
        duplicate_occurrences = valid_rows.filter(pl.col("_event_id") == duplicate_id).height
        duplicate_occurrences += rejected_rows.filter(pl.col("_event_id") == duplicate_id).height
        expected_max_occurrences = 1 if suffix == "_py" else 2
        assert duplicate_occurrences <= expected_max_occurrences, suffix


def test_python_and_sql_variants_produce_equivalent_content(test_pipeline_specs):
    full_spec_path, bronze_only_spec_path, spark_conf_dir, subprocess_cwd = test_pipeline_specs
    marker = uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)
    rows = [_base_row(f"{marker}-{i}", now) for i in range(5)]
    _write_raw_fixture(rows, now)

    bootstrap = _run_spark_pipelines_with_retry(bronze_only_spec_path, spark_conf_dir, subprocess_cwd)
    assert bootstrap.returncode == 0, bootstrap.stdout + bootstrap.stderr
    full_run = _run_spark_pipelines_with_retry(full_spec_path, spark_conf_dir, subprocess_cwd)
    assert full_run.returncode == 0, full_run.stdout + full_run.stderr

    py_ids = set(
        _read_rows(spark_conf_dir, "silver_recentchange_py")
        .filter(pl.col("_event_id").str.starts_with(marker))["_event_id"]
    )
    sql_ids = set(
        _read_rows(spark_conf_dir, "silver_recentchange_sql")
        .filter(pl.col("_event_id").str.starts_with(marker))["_event_id"]
    )
    assert py_ids == sql_ids == {f"{marker}-{i}" for i in range(5)}
