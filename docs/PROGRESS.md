# Progress

**Current phase:** Phase 2 — Bronze (`docs/SPEC-phase2-bronze.md`), on branch `phase2-bronze`.

**Implemented:** `bronze_recentchange` (Python-only SDP streaming table, Delta + CDF enabled) and `bronze_dim_wiki_reference` (Python + SQL materialized view variants, named `bronze_dim_wiki_reference_py`/`_sql` pending a pick between them), both contracts, `scripts/fetch_wiki_sitematrix.py`, `scripts/render_local_spark_config.py`, `scripts/inspect_bronze.py`, and all required tests. Verified end-to-end against real local MinIO and the real Wikimedia sitematrix API — dry-run, real execution with correct row counts, genuine Delta/CDF, true incremental reruns (no duplication), and Python/SQL variant equivalence. `SPEC-phase2-bronze.md` was substantially corrected during implementation against real, empirically-verified gaps between open-source Spark and Databricks (Sections 4.1–4.3) — no SDP expectations, no `read_files`, Delta-format/metastore quirks. `docs/RUNBOOK.md`, `docs/CLAUDE.md`, `README.md`, and `docs/trade-offs.md` updated accordingly.

**Left:** User is reviewing/testing locally (and will test on Databricks separately) before anything is committed. Once approved: export `requirements.txt`, commit, push `phase2-bronze`, open a PR, merge to `main` — then start Phase 2.5 (`docs/SPEC-phase2__5-cicd.md`).
