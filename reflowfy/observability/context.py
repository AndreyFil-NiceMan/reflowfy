"""Bind per-job context onto every log record via a LogRecordFactory + contextvars.

A record factory (not a logger Filter) is used so the fields land on records from
*any* logger, including children like 'reflowfy.worker.executor', regardless of
propagation.
"""

import contextvars
import logging
from contextlib import contextmanager
from typing import Any, Dict, Iterator

_ctx: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "reflowfy_log_ctx", default={}
)

_installed = False


def install_context_filter() -> None:
    """Install the context-injecting record factory. Idempotent."""
    global _installed
    if _installed:
        return
    old_factory = logging.getLogRecordFactory()

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = old_factory(*args, **kwargs)
        for key, val in _ctx.get().items():
            setattr(record, key, val)
        return record

    logging.setLogRecordFactory(factory)
    _installed = True


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind fields (execution_id, job_id, pipeline_name, ...) for the enclosed scope."""
    install_context_filter()
    token = _ctx.set({**_ctx.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _ctx.reset(token)
