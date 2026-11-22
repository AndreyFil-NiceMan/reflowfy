"""Shared by every per-record step (transformations, record destinations, the worker).

How many records are in flight at once, and how the reasons records were skipped are
counted. Kept in core so destinations and the worker need not import transformations.
"""

import logging
import os
from typing import Any, Dict, Mapping, Optional

DEFAULT_RECORD_CONCURRENCY = 16


def default_record_concurrency() -> int:
    """``TRANSFORM_CONCURRENCY`` (records in flight per per-record step), default 16."""
    raw = os.getenv("TRANSFORM_CONCURRENCY", "")
    try:
        value = int(raw) if raw.strip() else DEFAULT_RECORD_CONCURRENCY
    except ValueError:
        raise ValueError(f"TRANSFORM_CONCURRENCY must be an integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(f"TRANSFORM_CONCURRENCY must be >= 1, got {value}")
    return value


MAX_SKIP_REASONS = 20
"""Distinct ``SkipRecord`` reasons kept per step; the rest are counted as ``OTHER_SKIP_REASON``."""
MAX_SKIP_REASON_LENGTH = 200
OTHER_SKIP_REASON = "(other reasons)"


def count_skip_reason(reasons: Dict[str, int], reason: str, times: int = 1) -> None:
    """Add ``times`` skips for ``reason`` to ``reasons``, keeping the dict bounded."""
    reason = reason[:MAX_SKIP_REASON_LENGTH] or "(no reason)"
    if reason not in reasons and len(reasons) >= MAX_SKIP_REASONS:
        reason = OTHER_SKIP_REASON
    reasons[reason] = reasons.get(reason, 0) + times


def top_skip_reasons(reasons: Dict[str, int], limit: int = 3) -> str:
    """``"user inactive" x3, "no email" x1`` — the most frequent reasons, for one log line."""
    top = sorted(reasons.items(), key=lambda item: -item[1])[:limit]
    more = len(reasons) - len(top)
    text = ", ".join(f"{reason!r} x{times}" for reason, times in top)
    return f"{text}, +{more} more" if more > 0 else text


# Attributes every LogRecord has: an ``extra=`` key among them makes
# ``Logger.makeRecord`` raise ("Attempt to overwrite ..."), which would fail the job.
_LOG_RECORD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


def skip_log_extra(
    user_extra: Optional[Mapping[str, Any]], framework: Mapping[str, Any]
) -> Dict[str, Any]:
    """Merge a ``SkipJob``/``SkipRecord``'s ``extra`` with the framework's fields.

    The framework's fields (``event``, ``skip_reason``, …) keep their names, so the
    line is found by the usual queries. A user key that clashes with one of them, or
    with a LogRecord attribute (``message``, ``name``, ``args``…, which would make
    the log call raise), is kept as ``labels.<key>`` instead of being lost.
    """
    merged: Dict[str, Any] = {}
    for key, value in (user_extra or {}).items():
        name = str(key)
        if name in framework or name in _LOG_RECORD_ATTRS or name.startswith("_"):
            name = f"labels.{name.lstrip('_') or 'extra'}"
        merged[name] = value
    merged.update(framework)
    return merged


def log_record_skipped(
    log: Any, step: str, index: int, reason: str, extra: Mapping[str, Any]
) -> None:
    """The ``record_skipped`` line for one record dropped by ``SkipRecord(extra=...)``.

    Only a skip that carries ``extra`` gets one: the per-step ``records_skipped``
    line already counts the others, and a line per record is volume.
    """
    if not extra:
        return
    log.info(
        "%s skipped record %d (%s)",
        step,
        index,
        reason,
        extra=skip_log_extra(
            extra,
            {
                "event": "record_skipped",
                "transformation": step,
                "record_index": index,
                "skip_reason": reason,
            },
        ),
    )
