"""Wikipedia recentchange event parsing.

The single place that decides what's a known field vs. schema drift
(`_extra_fields`) — reused by producer, consumer, and later phases
(bronze/silver), per SPEC-phase1-ingestion.md section 6.

`parse_recentchange_event` only shapes the payload into known/extra fields
plus the fields it alone can derive from the payload (`_event_id`,
`_extra_fields`, `_schema_version`). Ingestion-context fields that only the
producer knows (`_ingested_at`, `_producer_instance`) are added by the
producer afterward, not here.
"""

import json

SCHEMA_VERSION = "1.0.0"

# SPEC-agnostic-architecture.md section 3 — the subset of the real
# recentchange payload this pipeline treats as "official." Everything else
# in the payload (today or added later by the source) is preserved,
# untyped, in `_extra_fields` — never dropped (Robustness Principle, P8).
KNOWN_TOP_LEVEL_FIELDS = {
    "id",
    "type",
    "title",
    "user",
    "bot",
    "wiki",
    "timestamp",
    "server_url",
    "meta",
    "length",
}


class MalformedEventError(Exception):
    """Raised when a raw SSE line can't be parsed into a recentchange event."""


def parse_recentchange_event(raw_line: str) -> dict:
    try:
        payload = json.loads(raw_line)
    except json.JSONDecodeError as exc:
        raise MalformedEventError(f"invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise MalformedEventError(f"expected a JSON object, got {type(payload).__name__}")

    event = {field: payload[field] for field in KNOWN_TOP_LEVEL_FIELDS if field in payload}
    extra_fields = {
        field: value for field, value in payload.items() if field not in KNOWN_TOP_LEVEL_FIELDS
    }

    event["_event_id"] = str(payload.get("id", ""))
    event["_extra_fields"] = json.dumps(extra_fields, default=str)
    event["_schema_version"] = SCHEMA_VERSION
    return event
