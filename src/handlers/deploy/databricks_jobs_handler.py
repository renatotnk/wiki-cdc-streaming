"""DatabricksJobsHandler -- the only class that talks to the Databricks Jobs
API (SPEC-phase2__5-cicd.md Section 6, convention 9.2). Decision logic
about when/what to deploy stays outside this class (that's
scripts/deploy_databricks_job.py's job) -- this handler only executes.

Uses `requests` directly against the Jobs REST API rather than adding the
`databricks-sdk` dependency for two calls (stdlib-first / P0 --
`requests` is already a project dependency, used by `WikiEventsHandler` and
`scripts/fetch_wiki_sitematrix.py`).
"""

import requests

JOBS_API_VERSION = "2.1"
REQUEST_TIMEOUT_SECONDS = 30


class DatabricksJobsHandler:
    def __init__(self, host: str, token: str) -> None:
        self._base_url = f"{host.rstrip('/')}/api/{JOBS_API_VERSION}/jobs"
        self._headers = {"Authorization": f"Bearer {token}"}

    def update_job_definition(self, job_id: str, notebook_path: str) -> None:
        """Points the Job's task at `notebook_path` -- the current branch's
        checked-out code (a Databricks Git folder, see docs/RUNBOOK.md
        Phase 2 Cloud section), so triggering a run afterward always
        executes what's on `main`, not a stale copy.
        """
        response = requests.post(
            f"{self._base_url}/update",
            headers=self._headers,
            json={
                "job_id": job_id,
                "new_settings": {
                    "tasks": [
                        {
                            "task_key": "wiki_cdc_pipeline",
                            "notebook_task": {"notebook_path": notebook_path},
                        }
                    ]
                },
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

    def trigger_run(self, job_id: str) -> str:
        response = requests.post(
            f"{self._base_url}/run-now",
            headers=self._headers,
            json={"job_id": job_id},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return str(response.json()["run_id"])
