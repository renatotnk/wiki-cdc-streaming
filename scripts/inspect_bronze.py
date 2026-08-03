"""Local bronze inspection via DuckDB's `delta_scan()` -- no SparkSession
needed (docs/SPEC-phase2-bronze.md Section 14). Prints row counts and a
sample of every bronze_* Delta table under the local warehouse.

IMPORTANT: read tables through delta_scan(), never by globbing the
warehouse directory's Parquet files directly -- a materialized view (e.g.
bronze_dim_wiki_reference) is refreshed via TRUNCATE + rewrite, and the
old files stay on disk as Delta tombstones until vacuumed; only
delta_scan() (or any Delta-transaction-log-aware reader) reports the
table's true current content.

Usage (from the repo root, for consistency with the other scripts, though
this one has no local-package import to break either way):
    python -m scripts.inspect_bronze
"""

from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_DIR = REPO_ROOT / "local-stack" / ".spark-conf" / "warehouse"


def _bronze_table_dirs() -> list[Path]:
    if not WAREHOUSE_DIR.is_dir():
        return []
    return sorted(p for p in WAREHOUSE_DIR.iterdir() if p.is_dir() and (p / "_delta_log").is_dir())


def _inspect_table(con: duckdb.DuckDBPyConnection, table_dir: Path) -> None:
    scan = f"delta_scan('{table_dir}')"
    count = con.execute(f"SELECT COUNT(*) FROM {scan}").fetchone()[0]
    print(f"\n=== {table_dir.name} ({count} rows) ===")
    print(con.execute(f"SELECT * FROM {scan} LIMIT 5").df())


def main() -> None:
    table_dirs = _bronze_table_dirs()
    if not table_dirs:
        print(
            f"No Delta tables found under {WAREHOUSE_DIR}. Run "
            "`python -m scripts.render_local_spark_config` and `spark-pipelines run` first."
        )
        return

    con = duckdb.connect()
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    for table_dir in table_dirs:
        _inspect_table(con, table_dir)


if __name__ == "__main__":
    main()
