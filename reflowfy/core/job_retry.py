"""Automatic job retries: :class:`JobRetryConfig` (opt-in, per pipeline).

A pipeline that sets ``job_retry`` has a failed job put back in the queue, after a
backoff, when its error looks transient (a source or destination that was down, a
timeout, a refused connection) instead of failing it at once::

    class Orders(AbstractPipeline[RuntimeParams]):
        name = "orders"
        job_retry = JobRetryConfig(max_retries=2, backoff_seconds=30)

The manager copies the config into every job message (``metadata["job_retry"]``) and the
worker that ran the job decides: the error matches and retries are left -> the job goes
back to ``pending`` with ``jobs.next_attempt_at = now + backoff``; otherwise it fails as
it always did. ``job_retry = None`` (the default) changes nothing.

This is not the job lease (``JOB_MAX_ATTEMPTS``): that one re-runs a job whose *worker*
disappeared (killed, OOM) and never saw an error. This one re-runs a job that *failed*.
"""

import asyncio
import importlib
import socket
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Type, cast
from reflowfy.core.exceptions import (
    DestinationError,
    PermanentDestinationError,
    RecordValidationError,
    SourceError,
    StepOutputError,
)

ExceptionTypes = Tuple[Type[BaseException], ...]

# How many earlier failures a job's stats keep (type + short message each).
ERROR_HISTORY_MAX = 5
ERROR_MESSAGE_MAX = 200


def default_retry_on() -> ExceptionTypes:
    """What is retried unless a pipeline says otherwise: errors of something outside the job.

    A source or destination that failed, a timeout, a refused or reset connection.
    """
    import httpx

    return (
        SourceError,
        DestinationError,
        TimeoutError,
        asyncio.TimeoutError,
        ConnectionError,
        socket.gaierror,
        httpx.TransportError,
    )


def default_never_retry() -> ExceptionTypes:
    """What is never retried: bad data and bugs, which fail the same way every time.

    ``never_retry`` wins over ``retry_on``: a ``PermanentDestinationError`` (an HTTP 4xx)
    is a ``DestinationError`` but is not retried. ``TemplateError`` is a ``ValueError``.
    """
    return (
        RecordValidationError,
        StepOutputError,
        PermanentDestinationError,
        KeyError,
        TypeError,
        ValueError,
    )


def _type_name(error_type: Type[BaseException]) -> str:
    return f"{error_type.__module__}.{error_type.__qualname__}"


def _load_type(name: str) -> Optional[Type[BaseException]]:
    """The exception class named ``module.Qual.Name``, or None if it cannot be found."""
    module_name, _, qualname = name.rpartition(".")
    # A nested class (module.Outer.Inner): walk back until a module imports.
    parts: List[str] = [qualname]
    while module_name:
        try:
            obj: Any = importlib.import_module(module_name)
        except ImportError:
            module_name, _, head = module_name.rpartition(".")
            parts.insert(0, head)
            continue
        for part in parts:
            obj = getattr(obj, part, None)
            if obj is None:
                return None
        if isinstance(obj, type) and issubclass(obj, BaseException):
            return obj
        return None
    return None


def error_chain(error: BaseException) -> List[BaseException]:
    """``error`` and what it wraps (``raise ... from`` and ``original_error``), outermost first.

    The framework wraps user errors (``pipeline_step`` turns a ``SourceError`` from a
    hook into a ``PipelineError``), so a match is looked for along the whole chain.
    """
    chain: List[BaseException] = []
    current: Optional[BaseException] = error
    while current is not None and current not in chain and len(chain) < 10:
        chain.append(current)
        wrapped = current.__cause__ or getattr(current, "original_error", None)
        current = wrapped if isinstance(wrapped, BaseException) else None
    return chain


def describe_error(error: BaseException) -> Dict[str, str]:
    """One ``error_history`` entry: the type and the start of the message."""
    message = str(error)
    if len(message) > ERROR_MESSAGE_MAX:
        message = message[: ERROR_MESSAGE_MAX - 1] + "…"
    return {"type": type(error).__name__, "message": message}


@dataclass(frozen=True)
class JobRetryConfig:
    """How a failed job is retried. Set it as a pipeline's ``job_retry``.

    Args:
        max_retries: Retries after the first failure (2 = up to 3 runs in all).
        backoff_seconds: Wait before the first retry.
        multiplier: Each further retry waits this many times longer.
        max_backoff_seconds: Longest wait.
        retry_on: Errors that are retried (anywhere in the error's cause chain).
            Default: :func:`default_retry_on`.
        never_retry: Errors that are never retried, even when they match ``retry_on``
            (wins). Default: :func:`default_never_retry`.

    A retried job runs again from the start: records it had already sent are sent again,
    unless the pipeline uses ``enable_duplicate_jobs = False``.
    """

    max_retries: int = 2
    backoff_seconds: float = 30.0
    multiplier: float = 2.0
    max_backoff_seconds: float = 600.0
    retry_on: ExceptionTypes = field(default_factory=default_retry_on)
    never_retry: ExceptionTypes = field(default_factory=default_never_retry)

    def __post_init__(self) -> None:
        max_retries = cast(object, self.max_retries)
        if isinstance(max_retries, bool) or not isinstance(max_retries, int):
            raise TypeError(f"JobRetryConfig.max_retries must be an int, got {self.max_retries!r}")
        if self.max_retries < 0:
            raise ValueError(f"JobRetryConfig.max_retries must be >= 0, got {self.max_retries}")
        if self.backoff_seconds < 0 or self.max_backoff_seconds < 0:
            raise ValueError("JobRetryConfig backoff seconds must be >= 0")
        if self.multiplier < 1:
            raise ValueError(f"JobRetryConfig.multiplier must be >= 1, got {self.multiplier}")
        for name in ("retry_on", "never_retry"):
            value: Any = getattr(self, name)
            if isinstance(value, type):
                value = (value,)  # a single class is accepted
                object.__setattr__(self, name, value)
            if not isinstance(value, tuple) or not all(
                isinstance(t, type) and issubclass(t, BaseException)
                for t in value  # pyright: ignore[reportUnknownVariableType]
            ):
                raise TypeError(f"JobRetryConfig.{name} must be a tuple of exception classes")

    def should_retry(self, error: BaseException) -> bool:
        """Whether this error may be retried (retries left is the caller's check)."""
        chain = error_chain(error)
        if any(isinstance(e, self.never_retry) for e in chain):
            return False
        return any(isinstance(e, self.retry_on) for e in chain)

    def delay_for(self, retry_number: int, error: Optional[BaseException] = None) -> float:
        """Seconds to wait before retry number ``retry_number`` (1 for the first).

        Exponential, capped at ``max_backoff_seconds``. A server that asked for more
        (an error with ``retry_after``, e.g. HTTP ``Retry-After``) is given it, up to
        the same cap.
        """
        exponent = max(retry_number - 1, 0)
        delay = min(self.backoff_seconds * (self.multiplier**exponent), self.max_backoff_seconds)
        for e in error_chain(error) if error is not None else []:
            retry_after = getattr(e, "retry_after", None)
            if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
                delay = max(delay, min(float(retry_after), self.max_backoff_seconds))
                break
        return float(delay)

    def to_dict(self) -> Dict[str, Any]:
        """JSON form carried in the job message (exception classes by import path)."""
        return {
            "max_retries": self.max_retries,
            "backoff_seconds": self.backoff_seconds,
            "multiplier": self.multiplier,
            "max_backoff_seconds": self.max_backoff_seconds,
            "retry_on": [_type_name(t) for t in self.retry_on],
            "never_retry": [_type_name(t) for t in self.never_retry],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobRetryConfig":
        """Rebuild the config from :meth:`to_dict`. An exception class the worker cannot
        import is left out (it could not have been raised there either)."""

        def types(names: Any) -> ExceptionTypes:
            listed: List[Any] = list(names or [])
            found = (_load_type(str(n)) for n in listed)
            return tuple(t for t in found if t is not None)

        return cls(
            max_retries=int(data.get("max_retries", 2)),
            backoff_seconds=float(data.get("backoff_seconds", 30.0)),
            multiplier=float(data.get("multiplier", 2.0)),
            max_backoff_seconds=float(data.get("max_backoff_seconds", 600.0)),
            retry_on=types(data.get("retry_on")),
            never_retry=types(data.get("never_retry")),
        )

    @classmethod
    def from_payload(cls, job_payload: Dict[str, Any]) -> Optional["JobRetryConfig"]:
        """The config a job message carries, or None (retries off)."""
        metadata: Any = job_payload.get("metadata")
        if not isinstance(metadata, dict):
            return None
        data: Any = cast(Dict[str, Any], metadata).get("job_retry")
        if not isinstance(data, dict):
            return None
        return cls.from_dict(cast(Dict[str, Any], data))
