"""Producer entry point: WikiEventsHandler -> dedup -> MessagingBackend.publish.

Business logic depends only on MessagingBackend (via backend_factory) and
WikiEventsHandler — never on a concrete Pub/Sub implementation (P2).
"""

import socket
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.handlers.wiki_events_handler import WikiEventsHandler
from src.producer.dedup_cache import EventIdLruCache
from src.shared.backend_factory import get_messaging_backend
from src.shared.constants import TOPIC_RECENTCHANGE_RAW
from src.shared.logger import get_logger

logger = get_logger("producer")


def run(
    wiki_handler: WikiEventsHandler | None = None,
    dedup_cache_size: int = 10_000,
    max_events: int | None = None,
) -> None:
    messaging_backend = get_messaging_backend()
    messaging_backend.ensure_topic(TOPIC_RECENTCHANGE_RAW)

    wiki_handler = wiki_handler or WikiEventsHandler()
    dedup_cache = EventIdLruCache(maxsize=dedup_cache_size)
    producer_instance = socket.gethostname()

    published_count = 0
    duplicate_count = 0
    for event in wiki_handler.events():
        event_id = event["_event_id"]
        if dedup_cache.has_seen(event_id):
            duplicate_count += 1
            logger.warning(
                "discarding duplicate event before publish",
                extra={"event_count": duplicate_count, "event_id": event_id},
            )
            continue
        dedup_cache.add(event_id)

        event["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        event["_producer_instance"] = producer_instance

        messaging_backend.publish(TOPIC_RECENTCHANGE_RAW, event)
        published_count += 1
        if published_count % 100 == 0:
            logger.info("published batch", extra={"event_count": published_count})

        if max_events is not None and published_count >= max_events:
            break

    logger.info(
        "producer stopped",
        extra={"event_count": published_count, "duplicate_count": duplicate_count},
    )


if __name__ == "__main__":
    load_dotenv()
    try:
        run()
    except KeyboardInterrupt:
        logger.info("producer interrupted by user, shutting down")
