"""Pulls a wiki-metadata snapshot from the Wikimedia sitematrix API and drops
it into dim_wiki_reference/ (a top-level prefix, deliberately *not* nested
under raw/ -- see docs/SPEC-phase2-bronze.md Section 3.1 for why) as a
timestamped NDJSON file, picked up by bronze_dim_wiki_reference on the next
`spark-pipelines run`. A one-off/periodic script, not part of the pipeline
itself -- run manually whenever you want a fresh snapshot.

Only the per-language entries are included (each grouping real content
wikis under a language, e.g. "aawiki"/"aawiktionary" under Afar) -- the
API's "specials" section (Wikidata, Commons, but also dozens of purely
organizational wikis like "board"/"steward"/"office") doesn't fit the
contract's "language_name" shape and is deliberately left out (P0/YAGNI).

Run as a module, from the repo root (not a direct file path -- otherwise
`from src...` resolves against scripts/'s own directory instead of the
repo root):
    python -m scripts.fetch_wiki_sitematrix
"""

import os
from datetime import datetime, timezone

import polars as pl
import requests
from dotenv import load_dotenv

from src.shared.backend_factory import get_storage_backend
from src.shared.logger import get_logger

SITEMATRIX_URL = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json"
USER_AGENT_TEMPLATE = "wiki-cdc-streaming/0.1.0 ({contact})"
PROJECT_CODE_TO_TYPE = {"wiki": "wikipedia"}  # other codes (wiktionary, wikibooks, ...) already match

logger = get_logger("fetch_wiki_sitematrix")


def _fetch_sitematrix() -> dict:
    user_agent = USER_AGENT_TEMPLATE.format(contact=os.environ["WIKI_STREAM_CONTACT"])
    response = requests.get(SITEMATRIX_URL, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return response.json()["sitematrix"]


def _parse_sitematrix(sitematrix: dict, fetched_at: str) -> list[dict]:
    records = []
    for key, language_entry in sitematrix.items():
        if key in ("count", "specials"):
            continue
        for site in language_entry.get("site", []):
            records.append(
                {
                    "wiki_code": site["dbname"],
                    "language_name": language_entry["localname"],
                    "project_type": PROJECT_CODE_TO_TYPE.get(site["code"], site["code"]),
                    "is_closed": "closed" in site,
                    "_snapshot_fetched_at": fetched_at,
                }
            )
    return records


def run() -> None:
    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    sitematrix = _fetch_sitematrix()
    records = _parse_sitematrix(sitematrix, fetched_at)

    storage_backend = get_storage_backend()
    path = f"dim_wiki_reference/snapshot-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    storage_backend.write(pl.DataFrame(records), path, format="ndjson")
    logger.info("wrote wiki reference snapshot", extra={"event_count": len(records), "path": path})


if __name__ == "__main__":
    load_dotenv()
    run()
