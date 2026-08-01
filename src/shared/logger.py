"""Structured JSON logging to stdout only (P5) — never to a file.

Reused by every phase: `get_logger(component)` returns a stdlib `Logger`
already wired to emit one JSON object per line. Extra structured fields
(e.g. `event_count`) are passed the normal `logging` way: `extra={...}`.
"""

import json
import logging
import sys
from datetime import datetime, timezone

_CONFIGURED_LOGGERS: set[str] = set()
_RESERVED_RECORD_ATTRS = set(vars(logging.makeLogRecord({})).keys())


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "component": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RESERVED_RECORD_ATTRS:
                payload[key] = value
        return json.dumps(payload, default=str)


def get_logger(component: str) -> logging.Logger:
    logger = logging.getLogger(component)
    if component not in _CONFIGURED_LOGGERS:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        _CONFIGURED_LOGGERS.add(component)
    return logger
