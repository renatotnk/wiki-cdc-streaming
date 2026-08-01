# CDC-Style Streaming Pipeline: Wikipedia EventStreams to a Medallion Lakehouse

A complete data pipeline — from ingesting Wikipedia's real-time change feed to a consumable medallion Lakehouse (bronze/silver/gold) — with every piece of infrastructure swappable between local and cloud execution via configuration, never via logic changes.

This project serves two purposes:

1. A public technical portfolio, reproducible by anyone without access to the author's cloud account.
2. A study vehicle for event ingestion, messaging, Delta Lake, Spark, Databricks, CI/CD, applied FinOps, dimensional modeling, and data visualization.

## Architecture

_Diagram placeholder — added once enough phases are implemented to make it worth drawing._

## Tech stack

- **Language:** Python 3.12, managed with [uv](https://docs.astral.sh/uv/)
- **Data source:** Wikipedia EventStreams (`recentchange`) via Server-Sent Events
- **Messaging:** Google Cloud Pub/Sub (Pub/Sub Emulator locally, real Pub/Sub optionally)
- **Storage:** MinIO locally, real AWS S3 or Google Cloud Storage optionally (S3 required for Databricks Free Edition's S3-only External Volumes)
- **Data manipulation:** [Polars](https://pola.rs/)
- **Compute:** [Spark Declarative Pipelines](https://spark.apache.org/) (`pyspark[pipelines]`, Spark 4.1+) + [Delta Lake](https://delta.io/), local via the `spark-pipelines` CLI, optionally as a Databricks Lakeflow Declarative Pipeline
- **Local inspection:** [DuckDB](https://duckdb.org/) (`delta_scan()`, no SparkSession needed)
- **Local infrastructure:** Docker Compose
- _(Databricks and Streamlit land with their respective phases below)_

## Phase status

- [x] Phase 1 — Ingestion (producer → messaging → raw bucket)
- [x] Phase 2 — Bronze (consolidation via Spark)
- [ ] Phase 2.5 — CI/CD
- [ ] Phase 3 — Silver (Data Quality + Change Data Feed)
- [ ] Phase 4 — Gold (Dimensional Modeling + Streamlit Dashboard)

## Documentation

- [`docs/SPEC-agnostic-architecture.md`](docs/SPEC-agnostic-architecture.md) — macro architecture, interface contracts, principles
- [`docs/SPEC-phase1-ingestion.md`](docs/SPEC-phase1-ingestion.md) onward — per-phase specs
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md) — step-by-step execution, local and cloud, with cost warnings
- [`docs/trade-offs.md`](docs/trade-offs.md) — decisions, alternatives, and what was sacrificed
- [`docs/ENGINEERING-PRINCIPLES.md`](docs/ENGINEERING-PRINCIPLES.md) — project-agnostic engineering principles
- [`docs/implementation-workflow.md`](docs/implementation-workflow.md) — repeatable per-phase implementation process
