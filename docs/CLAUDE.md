# CLAUDE.md

Wikipedia EventStreams → medallion Lakehouse (bronze/silver/gold): a CDC-style streaming pipeline, swappable between local and cloud execution via configuration only.

- Macro architecture, interface contracts, and principles P0–P8: `docs/SPEC-agnostic-architecture.md`
- Engineering principles (DRY/KISS/YAGNI/SOLID, applied with moderation): `docs/ENGINEERING-PRINCIPLES.md`
- Per-phase specs: `docs/SPEC-phase{N}-*.md`
- Repeatable implementation process: `docs/implementation-workflow.md`
- Current session checkpoint (volatile, refreshed every session): `docs/PROGRESS.md`

## Non-negotiable conventions

- Python is the default language for the entire implementation; dependencies are managed via `uv` (never `pip`/`venv`/`conda` directly).
- No hardcoded credentials — everything via `.env` + `python-dotenv`, loaded at each component's entry point.
- All repository content — code, comments, docstrings, docs — is written in English.

## What exists now

- **Phase 1 complete** — producer/consumer implemented (`WikiEventsHandler`, Pub/Sub emulator/cloud handlers, `S3CompatibleStorageHandler` serving `STORAGE_BACKEND=minio|s3` + `GcsStorageHandler` for `gcs`, `backend_factory`), see `SPEC-phase1-ingestion.md`.
- **Phase 2 complete** — `bronze_recentchange` (Python-only SDP streaming table) and `bronze_dim_wiki_reference` (Python + SQL materialized view variants, `_py`/`_sql` suffixed pending your pick) implemented and verified end-to-end against real local MinIO + the real Wikimedia sitematrix API. `SPEC-phase2-bronze.md` was corrected during implementation against several real, empirically-verified OSS Spark/Delta gaps vs. Databricks (no SDP expectations, no `read_files`, Delta-format quirks, metastore persistence) — read Section 4.1–4.3 before touching this phase's pipeline files or extending them in Phase 3. `scripts/render_local_spark_config.py` generates the local, gitignored `spark-pipeline.yml`/`spark-defaults.conf` from `.env` — always run it (and re-export `SPARK_CONF_DIR`) before any `spark-pipelines` command.
- **Phase 2.5 complete** — `.github/workflows/ci.yml`'s `test` job (Python + Java 17 setup, local stack, `spark-pipelines dry-run` before a real run, `pytest tests/`, `docker compose down -v` via `if: always()`) declares zero cloud secrets; `deploy` only runs via `workflow_dispatch` with `confirm_deploy=true`, gated behind `DATABRICKS_HOST`/`DATABRICKS_TOKEN` (secrets) and `DATABRICKS_JOB_ID`/`DATABRICKS_NOTEBOOK_PATH` (repo variables) that `test` never references. `DatabricksJobsHandler` (`src/handlers/deploy/`) talks to the Databricks **Jobs** API (not Pipelines directly) — a Job wrapping the Lakeflow pipeline must already exist. `tests/doubles/fake_wiki_events_handler.py` + `tests/fixtures/sample_recentchange_events.ndjson` (~50 events, incl. a bot event, a `log`-type event, a deliberate duplicate `_event_id`) are the only wiki-events source used in tests, aside from `tests/test_wiki_events_handler.py` unit-testing the real handler itself. Discovered running the full suite on a genuinely fresh bucket for the first time (see `RUNBOOK.md` Phase 2.5 for detail): both `raw/` and `dim_wiki_reference/` need at least one seeded object before the very first `dry-run`, and `test_ingestion_smoke.py` had to stop concatenating full raw-file schemas across test modules that don't share a fixture shape.
