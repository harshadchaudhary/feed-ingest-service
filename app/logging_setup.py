"""Structured JSON logging.

Logs are emitted as one JSON object per line so they can be queried by field
rather than grepped as prose. Interpolated prose is unsearchable once there
are several services writing to one place.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

# Attributes LogRecord always carries. Anything outside this set was added by
# the caller and belongs in the output as a structured field.
_STANDARD = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "module", "msecs",
    "message", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }

        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                payload[key] = value

        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Access logs duplicate what the app already records, at much higher volume.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
