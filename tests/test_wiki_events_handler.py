"""WikiEventsHandler: valid payload parsing and malformed payload handling
-- must not crash, must log WARNING and continue (SPEC section 12).
"""

import json
import logging

import pytest

from src.handlers.wiki_events_handler import WikiEventsHandler

VALID_EVENT = json.dumps(
    {
        "id": 42,
        "type": "edit",
        "title": "Example page",
        "user": "SomeUser",
        "bot": False,
        "wiki": "enwiki",
        "timestamp": 1234567890,
        "server_url": "https://en.wikipedia.org",
        "meta": {"dt": "2026-07-28T10:00:00Z"},
        "length": {"old": 100, "new": 150},
    }
)


@pytest.fixture
def handler_caplog(caplog):
    """WikiEventsHandler's logger has propagate=False (P5 clean stdout), so
    caplog needs to be attached to it directly rather than the root logger.
    """
    logger = logging.getLogger("wiki_events_handler")
    logger.addHandler(caplog.handler)
    yield caplog
    logger.removeHandler(caplog.handler)


def test_parses_a_valid_payload_into_an_event_dict():
    handler = WikiEventsHandler(event_source_factory=lambda: iter([VALID_EVENT]))

    event = next(handler.events())

    assert event["_event_id"] == "42"
    assert event["type"] == "edit"
    assert event["title"] == "Example page"
    assert event["meta"]["dt"] == "2026-07-28T10:00:00Z"


def test_skips_a_malformed_payload_without_crashing_and_logs_warning(handler_caplog):
    lines = ["not json{{{", VALID_EVENT]
    handler = WikiEventsHandler(event_source_factory=lambda: iter(lines))

    with handler_caplog.at_level(logging.WARNING, logger="wiki_events_handler"):
        event = next(handler.events())

    assert event["_event_id"] == "42"  # the valid event still comes through
    warnings = [r for r in handler_caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].message


def test_continues_yielding_events_after_a_malformed_payload(handler_caplog):
    lines = [VALID_EVENT, "not json{{{", VALID_EVENT]
    handler = WikiEventsHandler(event_source_factory=lambda: iter(lines))

    events = handler.events()
    first_event = next(events)
    second_event = next(events)  # generator must survive the malformed line in between

    assert first_event["_event_id"] == "42"
    assert second_event["_event_id"] == "42"
