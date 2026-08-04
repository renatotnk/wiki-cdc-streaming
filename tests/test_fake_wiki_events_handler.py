"""FakeWikiEventsHandler + its NDJSON fixture (SPEC-phase2__5-cicd.md
Section 3): the fixture carries the required edge cases (a bot event, a
log-type event, a deliberately duplicated `_event_id`), the double parses
every line via the same `parse_recentchange_event` the real handler uses,
and the duplicate flows correctly into the producer's dedup cache -- all
without a docker/network dependency (FIRST: Fast, Independent).
"""

from pathlib import Path

from src.producer.dedup_cache import EventIdLruCache
from tests.doubles.fake_wiki_events_handler import FakeWikiEventsHandler

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sample_recentchange_events.ndjson"


def test_fixture_has_around_50_events():
    events = list(FakeWikiEventsHandler(FIXTURE_PATH).events())

    assert len(events) == 50


def test_fixture_includes_a_bot_event():
    events = list(FakeWikiEventsHandler(FIXTURE_PATH).events())

    assert any(event["bot"] is True for event in events)


def test_fixture_includes_a_log_type_event():
    events = list(FakeWikiEventsHandler(FIXTURE_PATH).events())

    assert any(event["type"] == "log" for event in events)


def test_fixture_includes_a_deliberately_duplicated_event_id():
    events = list(FakeWikiEventsHandler(FIXTURE_PATH).events())

    event_ids = [event["_event_id"] for event in events]
    duplicated = {event_id for event_id in event_ids if event_ids.count(event_id) > 1}

    assert len(duplicated) == 1


def test_events_are_parsed_dicts_matching_the_real_handler_contract():
    first_event = next(FakeWikiEventsHandler(FIXTURE_PATH).events())

    # Same shape parse_recentchange_event() always produces (see
    # tests/test_wiki_events_handler.py for the equivalent assertion against
    # the real WikiEventsHandler).
    assert "_event_id" in first_event
    assert "_extra_fields" in first_event
    assert "_schema_version" in first_event
    assert first_event["type"] in ("edit", "new", "log", "categorize")


def test_producer_style_dedup_discards_the_fixtures_duplicate_event():
    """Mirrors src/producer/main.py's dedup loop (event -> has_seen? ->
    add()) without touching messaging/storage -- exercises exactly the
    scenario the fixture's duplicate _event_id pair exists for (SPEC
    Section 3): a redelivered event must be discarded, not published twice.
    """
    dedup_cache = EventIdLruCache(maxsize=1_000)
    events = list(FakeWikiEventsHandler(FIXTURE_PATH).events())

    accepted_event_ids = []
    duplicate_count = 0
    for event in events:
        event_id = event["_event_id"]
        if dedup_cache.has_seen(event_id):
            duplicate_count += 1
            continue
        dedup_cache.add(event_id)
        accepted_event_ids.append(event_id)

    assert duplicate_count == 1
    assert len(accepted_event_ids) == len(set(accepted_event_ids)) == 49
