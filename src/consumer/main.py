"""Consumer entry point: MessagingBackend.subscribe -> buffer -> StorageBackend.write.

Partitioning is derived from each event's own `meta.dt` (event time), never
from wall-clock write time — a late-arriving message must still land in
the partition matching when it happened, not when it was flushed.
"""

import os
import threading
import time
import uuid
from collections import defaultdict

import polars as pl
from dotenv import load_dotenv

from src.shared.backend_factory import get_messaging_backend, get_storage_backend
from src.shared.constants import SUBSCRIPTION_RECENTCHANGE_RAW, TOPIC_RECENTCHANGE_RAW
from src.shared.logger import get_logger

logger = get_logger("consumer")

DEFAULT_FLUSH_SIZE = 500
DEFAULT_FLUSH_INTERVAL_SECONDS = 60
FLUSH_CHECK_INTERVAL_SECONDS = 1


class RawEventBuffer:
    """Thread-safe buffer flushed by size or time, whichever comes first.
    `subscribe()` invokes `add()` concurrently from Pub/Sub's own worker
    threads, so access to the underlying list is lock-protected.
    """

    def __init__(self, storage_backend, flush_size: int) -> None:
        self._storage_backend = storage_backend
        self._flush_size = flush_size
        self._events: list[dict] = []
        self._lock = threading.Lock()

    def add(self, event: dict) -> None:
        with self._lock:
            self._events.append(event)
            should_flush = len(self._events) >= self._flush_size
        if should_flush:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            if not self._events:
                return
            batch, self._events = self._events, []
        self._write_batch(batch)

    def _write_batch(self, batch: list[dict]) -> None:
        deduped = _drop_duplicate_event_ids(batch)
        duplicate_count = len(batch) - len(deduped)
        if duplicate_count:
            logger.warning(
                "discarded duplicate events at flush time",
                extra={"event_count": duplicate_count},
            )

        for (date_part, hour_part), events in _group_by_partition(deduped).items():
            path = f"raw/dt={date_part}/hour={hour_part}/part-{uuid.uuid4()}.parquet"
            self._storage_backend.write(pl.DataFrame(events), path, format="parquet")
            logger.info("flushed partition", extra={"event_count": len(events), "path": path})


def _drop_duplicate_event_ids(events: list[dict]) -> list[dict]:
    seen: set[str] = set()
    deduped = []
    for event in events:
        event_id = event["_event_id"]
        if event_id in seen:
            continue
        seen.add(event_id)
        deduped.append(event)
    return deduped


def _partition_key(event: dict) -> tuple[str, str]:
    date_part, time_part = event["meta"]["dt"].split("T")
    return date_part, time_part[:2]


def _group_by_partition(events: list[dict]) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for event in events:
        groups[_partition_key(event)].append(event)
    return groups


def _run_periodic_flush(
    buffer: RawEventBuffer, flush_interval_seconds: int, stop_event: threading.Event
) -> None:
    last_flush = time.monotonic()
    while not stop_event.wait(FLUSH_CHECK_INTERVAL_SECONDS):
        if time.monotonic() - last_flush >= flush_interval_seconds:
            buffer.flush()
            last_flush = time.monotonic()


def run(flush_size: int | None = None, flush_interval_seconds: int | None = None) -> None:
    flush_size = flush_size or int(os.environ.get("CONSUMER_FLUSH_SIZE", DEFAULT_FLUSH_SIZE))
    flush_interval_seconds = flush_interval_seconds or int(
        os.environ.get("CONSUMER_FLUSH_INTERVAL_SECONDS", DEFAULT_FLUSH_INTERVAL_SECONDS)
    )

    messaging_backend = get_messaging_backend()
    storage_backend = get_storage_backend()
    messaging_backend.ensure_topic(TOPIC_RECENTCHANGE_RAW)  # idempotent; guarantees the subscription exists

    buffer = RawEventBuffer(storage_backend, flush_size=flush_size)
    stop_event = threading.Event()
    flush_thread = threading.Thread(
        target=_run_periodic_flush, args=(buffer, flush_interval_seconds, stop_event), daemon=True
    )
    flush_thread.start()

    try:
        messaging_backend.subscribe(SUBSCRIPTION_RECENTCHANGE_RAW, callback=buffer.add)
    finally:
        stop_event.set()
        buffer.flush()
        logger.info("consumer stopped, final flush complete")


if __name__ == "__main__":
    load_dotenv()
    try:
        run()
    except KeyboardInterrupt:
        logger.info("consumer interrupted by user, shutting down")
