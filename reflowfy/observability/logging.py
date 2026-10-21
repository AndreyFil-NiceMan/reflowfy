"""Structured (ECS) logging setup for reflowfy services.

Where logs go is driven entirely by env (LOG_DESTINATION):
  stdout   -> console only (default)
  elastic  -> the user's Elasticsearch only
  both     -> stdout + Elasticsearch
"""

import json
import logging
import os
import socket
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Attributes every LogRecord has. Anything else on a record came from `extra=` or the
# log_context() record factory, and is emitted — so a field passed to a log call can
# never be silently dropped for not being on an allowlist.
_STANDARD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}

# Context fields shown in the plain-text format, in this order.
_TEXT_CONTEXT = ("execution_id", "job_id", "pipeline_name", "event")

# Extra keys renamed to their ECS name. Everything else keeps its own name.
_ECS_RENAMES = {"event": "event.action", "reason": "event.reason"}


def _service_version() -> str:
    try:
        from reflowfy import __version__

        return str(__version__)
    except Exception:
        return "unknown"


class ECSJSONFormatter(logging.Formatter):
    """Render a LogRecord as a single ECS-shaped JSON line.

    Beyond the message it carries: where the line came from (service, version, host,
    pid, logger, file:line), when it happened (the record's own time, not the time it
    was formatted), the bound ``log_context`` / ``extra=`` fields (execution_id,
    job_id, pipeline_name, ...), ``event`` as ``event.action`` for a stable name to
    filter on, and ``error.type`` / ``error.message`` / ``error.stack_trace``.
    """

    def __init__(self, service_name: str = "reflowfy", environment: str = "local") -> None:
        super().__init__()
        self.service_name = service_name
        self.environment = environment
        self._static: Dict[str, Any] = {
            "service.name": service_name,
            "service.environment": environment,
            "service.version": _service_version(),
            "host.name": os.getenv("HOSTNAME") or socket.gethostname(),
            "process.pid": os.getpid(),
        }

    def format(self, record: logging.LogRecord) -> str:
        data: Dict[str, Any] = {
            "@timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "log.level": record.levelname.lower(),
            "logger": record.name,
            "log.origin.file.name": record.filename,
            "log.origin.file.line": record.lineno,
            "log.origin.function": record.funcName,
            "message": record.getMessage(),
            **self._static,
        }
        for key, val in record.__dict__.items():
            if key in _STANDARD_ATTRS or key.startswith("_") or val is None:
                continue
            data[_ECS_RENAMES.get(key, key)] = val
        if record.exc_info:
            exc_type, exc, _tb = record.exc_info
            if exc_type is not None:
                data["error.type"] = exc_type.__name__
            if exc is not None:
                data["error.message"] = str(exc)
            data["error.stack_trace"] = self.formatException(record.exc_info)
        # default=str: a stray non-serializable extra must not lose the whole line.
        return json.dumps(data, default=str)


class TextFormatter(logging.Formatter):
    """Human format (LOG_JSON=false) that still shows who/what the line is about."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        tags = [f"{k}={getattr(record, k)}" for k in _TEXT_CONTEXT if getattr(record, k, None)]
        return f"{line} [{' '.join(tags)}]" if tags else line


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
            ECSJSONFormatter(service_name, environment) if json_logs else TextFormatter()
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
