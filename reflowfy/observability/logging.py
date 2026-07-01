"""Structured logging configuration."""

import logging
import sys
import json
from typing import Any, Dict


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields
        if hasattr(record, "execution_id"):
            log_data["execution_id"] = getattr(record, "execution_id")
        if hasattr(record, "pipeline_name"):
            log_data["pipeline_name"] = getattr(record, "pipeline_name")
        if hasattr(record, "batch_id"):
            log_data["batch_id"] = getattr(record, "batch_id")

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_logging(json_logs: bool = False):
    """
    Setup logging configuration.

    Args:
        json_logs: Whether to use JSON formatting
    """
    logger = logging.getLogger("reflowfy")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)

    if json_logs:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )

    logger.addHandler(handler)

    return logger
