"""Manual entry point for the CI workflow's `deploy` job (and for running
the exact same step locally) -- the single place that decides *when* to
call DatabricksJobsHandler (convention 9.2: the handler only executes).

Refuses to run without `--confirm`, mirroring the workflow's own
`confirm_deploy=true` gate (SPEC-phase2__5-cicd.md Section 5) -- so this
script is just as safe to invoke by accident locally as the workflow is to
trigger by accident on GitHub.

Run as a module, from the repo root (same reasoning as every other script
in this project -- see scripts/fetch_wiki_sitematrix.py):
    python -m scripts.deploy_databricks_job --confirm
"""

import argparse
import os

from dotenv import load_dotenv

from src.handlers.deploy.databricks_jobs_handler import DatabricksJobsHandler
from src.shared.logger import get_logger

logger = get_logger("deploy_databricks_job")


def run(confirm: bool) -> None:
    if not confirm:
        raise SystemExit(
            "Refusing to deploy without --confirm -- deploy is a deliberate, "
            "explicit action (SPEC-phase2__5-cicd.md Section 5), never an "
            "accidental one."
        )

    job_id = os.environ["DATABRICKS_JOB_ID"]
    notebook_path = os.environ["DATABRICKS_NOTEBOOK_PATH"]
    handler = DatabricksJobsHandler(
        host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"]
    )

    handler.update_job_definition(job_id=job_id, notebook_path=notebook_path)
    run_id = handler.trigger_run(job_id=job_id)

    logger.info("triggered Databricks job run", extra={"job_id": job_id, "run_id": run_id})


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm", action="store_true", help="required -- refuses to run otherwise"
    )
    args = parser.parse_args()
    run(confirm=args.confirm)


if __name__ == "__main__":
    main()
