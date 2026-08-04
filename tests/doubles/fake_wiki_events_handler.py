"""FakeWikiEventsHandler -- test double for WikiEventsHandler
(SPEC-phase2__5-cicd.md Section 3).

Reads a frozen NDJSON fixture instead of connecting to the real Wikipedia
SSE, so tests that need a wiki-events source never touch
`stream.wikimedia.org`. This is the only source implementation used for
that purpose anywhere in tests/ -- `tests/test_wiki_events_handler.py` is
the one exception, since it unit-tests WikiEventsHandler's own parsing
logic and must exercise the real class.

Correction vs. the SPEC's draft pseudocode: it names the double's method
`stream_events()`, but the real WikiEventsHandler contract (already
implemented in Phase 1, `src/handlers/wiki_events_handler.py`) exposes
`events()`. This double mirrors the real method name instead, so it's a
genuine drop-in wherever `WikiEventsHandler` is used (e.g.
`producer.main.run(wiki_handler=...)`), not just a superficially similar
class.
"""

from collections.abc import Iterator
from pathlib import Path

from src.shared.event_schema import MalformedEventError, parse_recentchange_event
from src.shared.logger import get_logger

logger = get_logger("fake_wiki_events_handler")


class FakeWikiEventsHandler:
    def __init__(self, fixture_path: str | Path) -> None:
        self._fixture_path = Path(fixture_path)

    def events(self) -> Iterator[dict]:
        """Yields every fixture line parsed into an event dict, once, then
        stops -- unlike the real handler, this is a finite, exhaustible
        source (a fixture file, not a live stream), which is exactly what a
        deterministic test needs.
        """
        with self._fixture_path.open(encoding="utf-8") as fixture_file:
            for raw_line in fixture_file:
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    yield parse_recentchange_event(raw_line)
                except MalformedEventError as exc:
                    logger.warning(f"skipping malformed fixture line: {exc}")
                    continue
