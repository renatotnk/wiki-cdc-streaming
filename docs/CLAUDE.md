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
