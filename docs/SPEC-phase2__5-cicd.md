# SPEC — Phase 2.5: CI/CD

**Depends on:** `SPEC-agnostic-architecture.md` (principles P1–P7, conventions 9.1–9.4), `SPEC-phase1-ingestion.md` and `SPEC-phase2-bronze.md` (tested components).
**Does not redecide anything already fixed in those documents — only references them.**

---

## 1. Objective

Automatically validate, on every push to `main`, that the Phase 1 and Phase 2 components remain functional — using exclusively the local stack (no cloud credential, no cost). Deploying to Databricks is a separate, manual, explicit action, never triggered automatically by a push.

## 2. Scope

**In scope:**
- A GitHub Actions workflow that tests against `local-stack/docker-compose.yml`.
- A test strategy with no dependency on real external network (neither Wikipedia's SSE, nor real Pub/Sub/GCS).
- A manual deploy job (`workflow_dispatch` trigger) to update the Databricks Job.

**Out of scope:**
- Infrastructure provisioning (Terraform under `infra/terraform/` is applied manually, outside this workflow).
- Automatic deploy on every push (a deliberate decision — see Section 5).

## 3. Test strategy with no external network dependency

Testing against the real Wikipedia SSE in CI would be fragile (unstable network, non-deterministic data, variable run time). Instead, a **test double** of `WikiEventsHandler` is used, implementing the same contract but reading from a local fixture file:

```python
# tests/doubles/fake_wiki_events_handler.py
class FakeWikiEventsHandler:
    """Implements the same contract as WikiEventsHandler, but reads from an
    NDJSON fixture instead of connecting to the real SSE. Used exclusively in tests."""
    def __init__(self, fixture_path: str): ...
    def stream_events(self) -> Iterator[dict]: ...
```

- Fixture: `tests/fixtures/sample_recentchange_events.ndjson` — ~50 real events captured once from the SSE and frozen, including edge cases (an event with `bot=true`, an event of type `log`, a pair of events with a deliberately duplicated `_event_id` to exercise dedup).
- No test in the default CI makes a network call to `stream.wikimedia.org`, real Pub/Sub, or GCS. This is validated by the acceptance criterion itself (Section 7, item 2).

## 4. GitHub Actions workflow

```
.github/workflows/ci.yml
├── job: test (always, on every push/PR to main)
│   ├── checkout
│   ├── setup Python + Java (Spark needs a JVM)
│   ├── docker compose up -d (pubsub-emulator, minio) — local-stack/docker-compose.yml
│   ├── pip install -r requirements.txt
│   ├── pytest tests/ (uses FakeWikiEventsHandler, PubSubEmulatorHandler, S3CompatibleStorageHandler)
│   └── docker compose down -v (always runs, even if tests fail)
│
└── job: deploy (only via manual workflow_dispatch, input `confirm_deploy=true`)
    ├── checkout
    ├── build/package the code from src/transform_bronze (and silver/gold in the future)
    └── DatabricksJobsHandler.update_job(...) via the Databricks Jobs API
```

The `test` job runs on **every push/PR** to `main`. The `deploy` job **never** runs automatically — it only exists when someone manually triggers the workflow with the confirmation input, avoiding API calls or cost on every commit.

## 5. Why deploy is manual, not automatic

A decision already recorded in Section 7 of `SPEC-agnostic-architecture.md` (Phase 2.5) — reaffirmed here with the concrete mechanism: `workflow_dispatch` requiring `inputs.confirm_deploy` to be `true`. This prevents an accidental push (e.g., merging a PR unrelated to the pipeline) from triggering an update or a Job run on Databricks without explicit intent.

## 6. Deploy handler (convention 9.2)

```python
# src/handlers/deploy/databricks_jobs_handler.py
class DatabricksJobsHandler:
    """The only class that talks to the Databricks Jobs API. Decision logic
    about when/what to deploy stays outside — this handler only executes."""
    def __init__(self, host: str, token: str): ...
    def update_job_definition(self, job_id: str, notebook_path: str) -> None: ...
    def trigger_run(self, job_id: str) -> str: ...  # returns run_id
```

Invoked by `scripts/deploy_databricks_job.py`, the single entry point of the workflow's `deploy` job.

## 7. Configuration (GitHub Actions secrets)

| Secret/Variable | Used in | Required |
|---|---|---|
| — (none) | `test` job | No — runs 100% locally |
| `DATABRICKS_HOST` | `deploy` job | Yes, only when `deploy` is triggered |
| `DATABRICKS_TOKEN` | `deploy` job | Yes, only when `deploy` is triggered |

The `test` job **declares no secret at all** — this is auditable just by looking at `ci.yml` itself, and is part of the acceptance criteria.

## 8. Acceptance criteria (Given/When/Then)

1. **CI green with no cloud credential**
   Given a push to `main`, When the `test` job runs, Then all tests pass using only `local-stack/docker-compose.yml`, with no cloud secret referenced in the job.

2. **Zero external network calls in tests**
   Given the full test suite, When run in CI, Then no network call is made to `stream.wikimedia.org`, real Pub/Sub, or GCS — verifiable by `FakeWikiEventsHandler` being the only source implementation used in `tests/`.

3. **Deploy is not automatic**
   Given a regular push to `main` (without triggering `workflow_dispatch`), When CI runs, Then the `deploy` job does not execute.

4. **Manual deploy works**
   Given someone triggers `workflow_dispatch` with `confirm_deploy=true`, When the `deploy` job runs, Then `DatabricksJobsHandler.update_job_definition()` is called successfully and the Databricks Job reflects the `main` branch's code.

5. **Teardown always runs**
   Given any outcome of the `test` job (success or failure), When the workflow finishes, Then `docker compose down -v` executes (using `if: always()` on the step), with no containers or volumes left on the runner.

## 9. Code structure for this phase

```
.github/workflows/ci.yml
tests/
├── fixtures/
│   └── sample_recentchange_events.ndjson
└── doubles/
    └── fake_wiki_events_handler.py
src/handlers/deploy/
└── databricks_jobs_handler.py
scripts/
└── deploy_databricks_job.py
```

## 10. Operating commands (reproducing CI locally)

```bash
# Bring up the same stack CI uses
docker compose -f local-stack/docker-compose.yml up -d

# Run exactly the suite CI runs
pytest tests/

# Tear down
docker compose -f local-stack/docker-compose.yml down -v

# Manual deploy (equivalent to the workflow's "deploy" job, run locally)
python scripts/deploy_databricks_job.py --confirm
```
