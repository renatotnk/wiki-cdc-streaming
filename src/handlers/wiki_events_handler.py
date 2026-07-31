"""WikiEventsHandler — consumes the Wikipedia recentchange SSE stream.

Only talks to the source: connects, reconnects with backoff on drop, and
parses each line via the shared `parse_recentchange_event`. No business
logic (dedup, publishing) lives here — that's the producer's job
(convention 9.2).
"""

import os
import time
from collections.abc import Callable, Iterable, Iterator

import requests
import sseclient

from src.shared.event_schema import MalformedEventError, parse_recentchange_event
from src.shared.logger import get_logger

DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
INITIAL_BACKOFF_SECONDS = 1
MAX_BACKOFF_SECONDS = 60

# Wikimedia rejects requests with a generic/missing User-Agent (403) — see
# https://meta.wikimedia.org/wiki/User-Agent_policy. No default: the contact
# address is per-deployer and must come from .env, never hardcoded here.
USER_AGENT_TEMPLATE = "wiki-cdc-streaming/0.1.0 ({contact})"

logger = get_logger("wiki_events_handler")


class WikiEventsHandler:
    def __init__(
        self,
        stream_url: str | None = None,
        event_source_factory: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        """`event_source_factory` is a seam for tests: a zero-arg callable
        returning a fresh iterable of raw SSE data strings, standing in for
        `_open_sse_stream` without a real network connection.
        """
        self._stream_url = stream_url or os.environ.get("WIKI_STREAM_URL", DEFAULT_STREAM_URL)
        self._event_source_factory = event_source_factory or self._open_sse_stream

    def events(self) -> Iterator[dict]:
        """Yields parsed recentchange events forever, reconnecting with
        exponential backoff whenever the underlying source drops or ends.
        """
        backoff_seconds = INITIAL_BACKOFF_SECONDS
        while True:
            try:
                for event in self._consume_once():
                    yield event
                    backoff_seconds = INITIAL_BACKOFF_SECONDS
            except (requests.exceptions.RequestException, OSError) as exc:
                logger.warning(
                    f"SSE connection dropped, reconnecting in {backoff_seconds}s: {exc}",
                )
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, MAX_BACKOFF_SECONDS)

    def _consume_once(self) -> Iterator[dict]:
        for raw_line in self._event_source_factory():
            if not raw_line:
                continue
            try:
                yield parse_recentchange_event(raw_line)
            except MalformedEventError as exc:
                logger.warning(f"skipping malformed event: {exc}")
                continue

    def _open_sse_stream(self) -> Iterator[str]:
        user_agent = USER_AGENT_TEMPLATE.format(contact=os.environ["WIKI_STREAM_CONTACT"])
        response = requests.get(
            self._stream_url, stream=True, headers={"User-Agent": user_agent}
        )
        response.raise_for_status()
        client = sseclient.SSEClient(response)
        for sse_event in client.events():
            yield sse_event.data
