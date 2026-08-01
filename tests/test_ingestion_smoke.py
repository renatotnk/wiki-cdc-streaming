"""Integration smoke test: producer -> messaging -> consumer -> storage
against the real local stack (SPEC section 12 and section 2 acceptance
criterion 1: published count == persisted count, no loss).

Requires `docker compose -f local-stack/docker-compose.yml up -d`
beforehand (docs/implementation-workflow.md step 6). Skipped, not failed,
if the local stack isn't reachable, so `pytest tests/ -v` stays usable
without Docker running.
"""

import json
import os
import threading
import time
import uuid
from urllib.parse import urlparse

import polars as pl
import pytest
from minio import Minio

from src.consumer import main as consumer_main
from src.handlers.wiki_events_handler import WikiEventsHandler
from src.producer import main as producer_main
from src.shared.backend_factory import get_messaging_backend, get_storage_backend
from src.shared.constants import TOPIC_RECENTCHANGE_RAW

EVENT_COUNT = 20
POLL_TIMEOUT_SECONDS = 15


def _local_stack_available() -> bool:
    try:
        get_messaging_backend().ensure_topic(TOPIC_RECENTCHANGE_RAW)
        get_storage_backend()
        return True
    except Exception:
        # Any failure here means "can't reach the local stack" -- treated
        # uniformly as unavailable so the test skips instead of erroring.
        return False


pytestmark = pytest.mark.skipif(
    not _local_stack_available(),
    reason="local stack (Pub/Sub Emulator + MinIO) is not reachable -- run "
    "`docker compose -f local-stack/docker-compose.yml up -d` first",
)


def _make_event(i: int, run_marker: str) -> str:
    return json.dumps(
        {
            "id": i,
            "type": "edit",
            "title": f"smoke-test-{i}",
            "user": run_marker,
            "bot": False,
            "wiki": "enwiki",
            "timestamp": 1_700_000_000 + i,
            "server_url": "https://en.wikipedia.org",
            "meta": {"dt": "2026-07-28T09:00:00Z"},
            "length": {"old": 1, "new": 2},
        }
    )


def _read_all_partitions(storage_backend) -> pl.DataFrame:
    endpoint = urlparse(os.environ["S3_ENDPOINT_URL"])
    client = Minio(
        endpoint.netloc,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        secure=endpoint.scheme == "https",
    )
    objects = client.list_objects(os.environ["BUCKET_NAME"], prefix="raw/", recursive=True)
    frames = [storage_backend.read(obj.object_name, format="parquet") for obj in objects]
    return pl.concat(frames) if frames else pl.DataFrame()


def _persisted_count(storage_backend, run_marker: str) -> int:
    rows = _read_all_partitions(storage_backend)
    if rows.is_empty():
        return 0
    return rows.filter(pl.col("user") == run_marker).height


def _wait_until_persisted(storage_backend, run_marker: str, target: int) -> int:
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    count = 0
    while time.monotonic() < deadline:
        count = _persisted_count(storage_backend, run_marker)
        if count >= target:
            break
        time.sleep(0.5)
    return count


def test_published_count_equals_persisted_count():
    run_marker = f"smoke-{uuid.uuid4().hex[:8]}"
    lines = [_make_event(i, run_marker) for i in range(EVENT_COUNT)]
    fake_wiki_handler = WikiEventsHandler(event_source_factory=lambda: iter(lines))

    # Ensures the subscription exists before anything is published -- a
    # message published before the subscription exists is lost to it
    # forever, not just delayed.
    get_messaging_backend().ensure_topic(TOPIC_RECENTCHANGE_RAW)

    consumer_thread = threading.Thread(
        target=consumer_main.run,
        kwargs={"flush_size": EVENT_COUNT, "flush_interval_seconds": 2},
        daemon=True,
    )
    consumer_thread.start()
    time.sleep(1)  # let the streaming pull attach

    producer_main.run(wiki_handler=fake_wiki_handler, max_events=EVENT_COUNT)

    storage_backend = get_storage_backend()
    persisted_count = _wait_until_persisted(storage_backend, run_marker, EVENT_COUNT)

    assert persisted_count == EVENT_COUNT
