"""Describing an HTTP error response without its body.

A response body can echo the request (record values, tokens) and can be megabytes
long, so an error message carries only its status, type and size:
``HTTP 404 Not Found (application/json, 812 bytes)``. The body goes to a debug log,
truncated to :data:`BODY_LOG_CHARS` characters.
"""

import email.utils
import logging
import math
import time
from datetime import timezone
from typing import Optional

import httpx

BODY_LOG_CHARS = 200

RETRYABLE_4XX = frozenset({408, 425, 429})
"""4xx statuses that are worth retrying: request timeout, too early, too many requests."""


def describe_response(response: httpx.Response) -> str:
    """``HTTP 404 Not Found (application/json, 812 bytes)`` — never the body."""
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
    try:
        size = len(response.content)
    except httpx.ResponseNotRead:
        size = int(response.headers.get("content-length", "0") or 0)
    parts = [p for p in (content_type, f"{size} bytes") if p]
    reason = f" {response.reason_phrase}" if response.reason_phrase else ""
    return f"HTTP {response.status_code}{reason} ({', '.join(parts)})"


def log_response_body(logger: logging.Logger, response: httpx.Response, owner: str) -> None:
    """The start of an error response's body, at debug level only."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    try:
        body = response.text[:BODY_LOG_CHARS]
    except Exception:  # not read, or undecodable: the summary already says enough
        return
    logger.debug(
        "%s: HTTP %d response body (first %d chars): %s",
        owner,
        response.status_code,
        BODY_LOG_CHARS,
        body,
        extra={"event": "http_error_body", "status_code": response.status_code},
    )


def is_permanent(status_code: int) -> bool:
    """A 4xx that sending the same request again cannot fix."""
    return 400 <= status_code < 500 and status_code not in RETRYABLE_4XX


def retry_after_seconds(response: httpx.Response) -> Optional[float]:
    """The ``Retry-After`` of a 429/503 in seconds (delay or HTTP date), else None."""
    if response.status_code not in (429, 503):
        return None
    value = response.headers.get("retry-after")
    if not value:
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except ValueError:
        pass
    else:
        return max(seconds, 0.0) if math.isfinite(seconds) else None
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(when.timestamp() - time.time(), 0.0)


__all__ = [
    "BODY_LOG_CHARS",
    "RETRYABLE_4XX",
    "describe_response",
    "is_permanent",
    "log_response_body",
    "retry_after_seconds",
]
