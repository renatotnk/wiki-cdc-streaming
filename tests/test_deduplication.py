"""EventIdLruCache correctly discards redeliveries (SPEC section 12,
acceptance criterion 3)."""

from src.producer.dedup_cache import EventIdLruCache


def test_a_new_event_id_is_not_flagged_as_seen():
    cache = EventIdLruCache(maxsize=10)

    assert cache.has_seen("99") is False


def test_an_added_event_id_is_flagged_as_seen():
    cache = EventIdLruCache(maxsize=10)

    cache.add("42")

    assert cache.has_seen("42") is True


def test_evicts_the_oldest_id_once_maxsize_is_exceeded():
    cache = EventIdLruCache(maxsize=2)
    cache.add("1")
    cache.add("2")

    cache.add("3")  # evicts "1", the oldest

    assert cache.has_seen("1") is False
    assert cache.has_seen("2") is True
    assert cache.has_seen("3") is True


def test_simulated_reconnection_redelivering_the_last_50_events_is_fully_discarded():
    cache = EventIdLruCache(maxsize=10_000)
    original_ids = [str(i) for i in range(1000)]
    for event_id in original_ids:
        cache.add(event_id)

    redelivered_ids = [str(i) for i in range(950, 1000)]  # last 50, redelivered on reconnect

    assert all(cache.has_seen(event_id) for event_id in redelivered_ids)
