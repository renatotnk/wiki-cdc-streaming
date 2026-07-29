"""LRU cache of recently seen `_event_id`s — defense in depth against SSE
reconnection redelivery (SPEC-phase1-ingestion.md section 7). Definitive
dedup still happens in the consumer, at flush time.
"""

from collections import deque

DEFAULT_MAXSIZE = 10_000


class EventIdLruCache:
    def __init__(self, maxsize: int = DEFAULT_MAXSIZE) -> None:
        self._maxsize = maxsize
        self._order: deque[str] = deque()
        self._seen: set[str] = set()

    def has_seen(self, event_id: str) -> bool:
        return event_id in self._seen

    def add(self, event_id: str) -> None:
        if event_id in self._seen:
            return
        self._seen.add(event_id)
        self._order.append(event_id)
        if len(self._order) > self._maxsize:
            oldest = self._order.popleft()
            self._seen.discard(oldest)
