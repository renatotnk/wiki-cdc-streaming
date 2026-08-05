"""Local inspection of silver_recentchange_rejected via DuckDB's
`delta_scan()` -- no SparkSession needed (same technique as
scripts/inspect_bronze.py). Prints row counts, a breakdown of
`_dq_failure_reasons`, and a sample of rejected rows for every
silver_recentchange_rejected* table under the local warehouse (both the
`_py` and `_sql` variants, whichever exist).

Usage (from the repo root):
    python -m scripts.inspect_silver_rejected
"""

from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
WAREHOUSE_DIR = REPO_ROOT / "local-stack" / ".spark-conf" / "warehouse"


def _rejected_table_dirs() -> list[Path]:
    if not WAREHOUSE_DIR.is_dir():
        return []
    return sorted(
        p
        for p in WAREHOUSE_DIR.iterdir()
        if p.is_dir() and p.name.startswith("silver_recentchange_rejected") and (p / "_delta_log").is_dir()
    )


def _inspect_table(con: duckdb.DuckDBPyConnection, table_dir: Path) -> None:
    scan = f"delta_scan('{table_dir}')"
    count = con.execute(f"SELECT COUNT(*) FROM {scan}").fetchone()[0]
    print(f"\n=== {table_dir.name} ({count} rows) ===")
    if count == 0:
        return
    print("\n-- rejection reason breakdown --")
    print(
        con.execute(
            f"SELECT reason, COUNT(*) AS row_count FROM "
            f"(SELECT UNNEST(_dq_failure_reasons) AS reason FROM {scan}) "
            f"GROUP BY reason ORDER BY row_count DESC"
        ).df()
    )
    print("\n-- sample rows --")
    print(con.execute(f"SELECT _event_id, type, _dq_failure_reasons FROM {scan} LIMIT 5").df())


def main() -> None:
    table_dirs = _rejected_table_dirs()
    if not table_dirs:
        print(
            f"No silver_recentchange_rejected* table found under {WAREHOUSE_DIR}. "
            "Run the silver pipeline first (docs/RUNBOOK.md, Phase 3)."
        )
        return

    con = duckdb.connect()
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    for table_dir in table_dirs:
        _inspect_table(con, table_dir)


if __name__ == "__main__":
    main()
