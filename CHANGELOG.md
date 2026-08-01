# Changelog

All notable changes to this project are documented here. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [Semantic Versioning](https://semver.org/) — each phase lands as a `MINOR` release (this is a portfolio/study project with no external consumers, so there's no `PATCH`-worthy public API to break; a `1.0.0` marks the first fully complete pipeline, Phases 1–4).

## [Unreleased]

## [0.2.0] — Phase 2: Bronze — 2026-08-01

SDP-based bronze layer over Phase 1's raw output, plus a second, genuinely external reference dimension. See `docs/SPEC-phase2-bronze.md` and `docs/trade-offs.md` for the substantial corrections made against real, empirically-verified gaps between open-source Spark and Databricks discovered during implementation (no SDP expectations, no `read_files`, Delta-format/local-metastore quirks).

### Added
- `bronze_recentchange`: SDP streaming table (Python-only — no SQL variant is possible for this dataset in open-source Spark), Delta format with Change Data Feed enabled, reading Phase 1's raw Parquet output.
- `bronze_dim_wiki_reference`: SDP batch materialized view, Python **and** SQL variants (`bronze_dim_wiki_reference_py`/`_sql`, pending a pick between them), sourced from the Wikimedia sitematrix API.
- `contracts/bronze_recentchange.contract.yaml`, `contracts/dim_wiki_reference.contract.yaml`.
- `scripts/fetch_wiki_sitematrix.py` — pulls a wiki-metadata snapshot from the Wikimedia API.
- `scripts/render_local_spark_config.py` — generates the local, gitignored `spark-pipeline.yml`/`spark-defaults.conf` from `.env`.
- `scripts/inspect_bronze.py` — local bronze inspection via DuckDB's `delta_scan()`.
- `tests/test_bronze_pipeline.py`, `tests/test_dim_wiki_reference.py`.
- `ndjson` support in `S3CompatibleStorageHandler`/`GcsStorageHandler`, alongside Phase 1's `parquet`.

### Changed
- `docs/SPEC-phase2-bronze.md` corrected during implementation (Sections 4.1–4.3): expectations dropped from this phase entirely (deferred to Phase 3); `bronze_recentchange`'s SQL variant dropped (not expressible in OSS Spark); the wiki-reference snapshot path moved from `raw/dim_wiki_reference/` to a sibling `dim_wiki_reference/` prefix (avoids a Hive-partition-discovery conflict with `bronze_recentchange`'s read of the whole `raw/` tree).

PR: _pending_ (not yet opened)

## [0.1.0] — Phase 1: Ingestion — 2026-08-01

Producer → messaging → raw bucket ingestion pipeline, with pluggable messaging and storage backends.

### Added
- `WikiEventsHandler` — consumes the Wikipedia `recentchange` EventStreams SSE feed.
- `MessagingBackend` interface: `PubSubEmulatorHandler` (local, default) and `PubSubCloudHandler` (real Pub/Sub).
- `StorageBackend` interface: `S3CompatibleStorageHandler` (serving both `minio` and real `s3`) and `GcsStorageHandler`.
- `backend_factory` — single point of backend selection from `.env`.
- Producer entry point (SSE → dedup → publish) and consumer entry point (subscribe → buffer → partitioned Parquet write).
- Event deduplication: producer-side LRU cache plus consumer-side flush-time dedup.
- Structured JSON logging (`src/shared/logger.py`, P5).
- Required tests (SPEC section 12), including a real local-stack smoke test.

### Fixed
- Non-deterministic column order in `event_schema.py` (caught by a repeatability check — the smoke test run twice back-to-back).

PR: [#1](https://github.com/renatotnk/wiki-cdc-streaming/pull/1)
