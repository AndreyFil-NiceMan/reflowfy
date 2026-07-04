"""Structured (ECS) logging setup for reflowfy services.

Where logs go is driven entirely by env (LOG_DESTINATION):
  stdout   -> console only (default)
  elastic  -> the user's Elasticsearch only
  both     -> stdout + Elasticsearch
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Extra LogRecord attributes we promote to top-level ECS-ish fields.
_CONTEXT_FIELDS = (
    "execution_id",
    "job_id",
    "pipeline_name",
    "batch_id",
    "trace.id",
    "span.id",
    "otelTraceID",
    "otelSpanID",
)


class ECSJSONFormatter(logging.Formatter):
    """Render a LogRecord as a single ECS-shaped JSON line."""

    def __init__(self, service_name: str = "reflowfy", environment: str = "local") -> None:
        super().__init__()
        self.service_name = service_name
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        data: Dict[str, Any] = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "log.level": record.levelname.lower(),
            "logger": record.name,
            "service.name": self.service_name,
            "service.environment": self.environment,
            "message": record.getMessage(),
        }
        for field in _CONTEXT_FIELDS:
            val = getattr(record, field, None)
            if val is not None:
                data[field] = val
        if record.exc_info:
            data["error.stack_trace"] = self.formatException(record.exc_info)
        return json.dumps(data)


def setup_logging(service_name: str = "reflowfy") -> logging.Logger:
    """Configure the root 'reflowfy' logger. Idempotent per process.

    Honors LOG_DESTINATION (stdout|elastic|both), LOG_LEVEL, LOG_JSON.
    """
    logger = logging.getLogger("reflowfy")
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logger.setLevel(level)

    # Idempotent: drop our previously-installed handlers before re-adding.
    for h in list(logger.handlers):
        if getattr(h, "_reflowfy_managed", False):
            logger.removeHandler(h)

    destination = os.getenv("LOG_DESTINATION", "stdout").lower()
    json_logs = os.getenv("LOG_JSON", "true").lower() == "true"
    environment = os.getenv("ENVIRONMENT", "local")

    want_stdout = destination in ("stdout", "both")
    want_elastic = destination in ("elastic", "both")

    # Guard the footgun: LOG_DESTINATION=elastic with no ELASTIC_LOG_URL would
    # attach an Elastic handler with no client and silently drop every log.
    # Fall back to stdout so logs are never black-holed.
    elastic_misconfigured = want_elastic and not os.getenv("ELASTIC_LOG_URL")
    if elastic_misconfigured:
        want_elastic = False
        want_stdout = True

    # An unrecognized value (typo) must not leave the logger with no handlers.
    unknown_destination = destination not in ("stdout", "elastic", "both")
    if unknown_destination:
        want_stdout = True

    if want_stdout:
        stdout = logging.StreamHandler(sys.stdout)
        stdout.setFormatter(
            ECSJSONFormatter(service_name, environment)
            if json_logs
            else logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        stdout._reflowfy_managed = True  # type: ignore[attr-defined]
        logger.addHandler(stdout)

    if want_elastic:
        from reflowfy.observability.elastic_handler import ElasticLogHandler

        es_handler = ElasticLogHandler(service_name=service_name)
        es_handler.setFormatter(ECSJSONFormatter(service_name, environment))
        es_handler._reflowfy_managed = True  # type: ignore[attr-defined]
        logger.addHandler(es_handler)

    logger.propagate = False

    if elastic_misconfigured:
        logger.warning(
            "LOG_DESTINATION=%s requested but ELASTIC_LOG_URL is empty; "
            "falling back to stdout logging.",
            destination,
        )
    if unknown_destination:
        logger.warning(
            "Unrecognized LOG_DESTINATION=%r (expected stdout|elastic|both); "
            "defaulting to stdout.",
            destination,
        )
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a child of the configured 'reflowfy' logger."""
    return logging.getLogger(f"reflowfy.{name}" if name else "reflowfy")
