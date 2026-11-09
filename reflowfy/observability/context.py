"""Bind per-job context onto every log record via a LogRecordFactory + contextvars.

A record factory (not a logger Filter) is used so the fields land on records from
*any* logger, including children like 'reflowfy.worker.executor', regardless of
propagation.
"""

import contextvars
import functools
import inspect
import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

_ctx: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "reflowfy_log_ctx", default={}
)

_installed = False


class _ContextFieldsDict(Dict[str, Any]):
    """A LogRecord's ``__dict__`` whose context fields do not count as taken.

    ``Logger.makeRecord`` raises ``KeyError("Attempt to overwrite ...")`` when an
    ``extra=`` key is already on the record — and the factory below has already put
    every bound context field there. So ``logger.info(..., extra={"pipeline_name": n})``
    inside ``log_context(pipeline_name=n)`` raised, and a log line failed the whole
    run. Context fields are defaults: a call's own ``extra`` replaces them.
    """

    context_keys: frozenset[str] = frozenset()

    def __contains__(self, key: object) -> bool:
        if key in self.context_keys:
            return False
        return super().__contains__(key)


def install_context_filter() -> None:
    """Install the context-injecting record factory. Idempotent."""
    global _installed
    if _installed:
        return
    old_factory = logging.getLogRecordFactory()

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = old_factory(*args, **kwargs)
        fields = _ctx.get()
        if fields:
            values = _ContextFieldsDict(record.__dict__)
            values.update(fields)
            values.context_keys = frozenset(fields)
            record.__dict__ = values
        return record

    logging.setLogRecordFactory(factory)
    _installed = True


@contextmanager
def log_context(**fields: Any) -> Generator[None, None, None]:
    """Bind fields (execution_id, job_id, pipeline_name, ...) for the enclosed scope."""
    install_context_filter()
    token = _ctx.set({**_ctx.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _ctx.reset(token)


def get_log_context() -> Dict[str, Any]:
    """The fields currently bound by log_context() (a copy)."""
    return dict(_ctx.get())


def bind_log_context(*names: str) -> Callable[[F], F]:
    """Decorator: bind the named arguments of the wrapped function as log context.

    ``@bind_log_context("execution_id", "pipeline_name")`` puts those two on every
    record logged inside the call (and what it awaits), so the message needn't repeat
    them and Elasticsearch can filter on them. Arguments the function doesn't take, or
    that are None, are skipped. Works on sync and async functions.
    """

    def decorate(fn: F) -> F:
        sig = inspect.signature(fn)

        def fields(args: Any, kwargs: Any) -> Dict[str, Any]:
            bound = sig.bind_partial(*args, **kwargs).arguments
            return {n: bound[n] for n in names if n in bound}

        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def awrapper(*args: Any, **kwargs: Any) -> Any:
                with log_context(**fields(args, kwargs)):
                    return await fn(*args, **kwargs)

            return awrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with log_context(**fields(args, kwargs)):
                return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorate
