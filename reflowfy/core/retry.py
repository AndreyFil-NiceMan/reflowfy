"""Step retries: one :class:`RetryConfig` for a source's fetch, a transformation and a
destination's send.

A step retry happens *inside* the job, right away (seconds), around the one call that
failed::

    elastic_source(..., retry=RetryConfig(max_attempts=4))        # the fetch
    @transformation(per_record=True, retry=RetryConfig())          # each record's call
    kafka_destination(..., retry_config=RetryConfig(max_attempts=5))  # the send

When the attempts run out the error goes on as before: it fails the job (or, for a
per-record step with ``on_error="skip"``, skips the record), and the pipeline's
``job_retry`` (:mod:`reflowfy.core.job_retry`) may then re-run the whole job later.

What is retried: ``retry_on`` minus ``never_retry`` (which wins), matched along the
error's cause chain. Left ``None`` they are the step's defaults — for a source or a
transformation the ``job_retry`` defaults (:func:`default_retry_on` /
:func:`default_never_retry`: errors of something outside the job, never bad data or
bugs); for a destination a ``DestinationError`` that is not a
``PermanentDestinationError``. ``SkipJob`` and ``SkipRecord`` are never retried.

Every retry is logged (``step_retry``) and counted per step for the job being run
(:func:`count_retries`), which the worker stores in the job's stats
(``step_retries``) and counts as ``reflowfy_step_retries_total{pipeline, step}``.
"""

import asyncio
import logging
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeVar,
    cast,
)

from reflowfy.core.exceptions import (
    DestinationError,
    PermanentDestinationError,
    SkipJob,
    SkipRecord,
)
from reflowfy.core.job_retry import (
    ExceptionTypes,
    _load_type,  # pyright: ignore[reportPrivateUsage]
    _type_name,  # pyright: ignore[reportPrivateUsage]
    default_never_retry,
    default_retry_on,
    error_chain,
)

__all__ = [
    "RetryConfig",
    "RetryDefaults",
    "count_retries",
    "destination_defaults",
    "retry_async",
    "retry_sync",
    "step_defaults",
]

logger = logging.getLogger(__name__)

T = TypeVar("T")

RetryDefaults = Tuple[ExceptionTypes, ExceptionTypes]
"""``(retry_on, never_retry)`` a step uses where its :class:`RetryConfig` leaves them ``None``."""


def step_defaults() -> RetryDefaults:
    """A source's or a transformation's defaults: the same as ``job_retry``'s."""
    return default_retry_on(), default_never_retry()


def destination_defaults() -> RetryDefaults:
    """A destination's defaults: a ``DestinationError``, unless it is permanent (a 4xx)."""
    return (DestinationError,), (PermanentDestinationError,)


def _check_types(name: str, value: Any) -> Optional[ExceptionTypes]:
    if value is None:
        return None
    if isinstance(value, type):
        value = (value,)  # a single class is accepted
    if isinstance(value, list):
        value = tuple(cast(List[Any], value))
    if not isinstance(value, tuple) or not all(
        isinstance(t, type) and issubclass(t, BaseException) for t in cast(Tuple[Any, ...], value)
    ):
        raise TypeError(f"RetryConfig.{name} must be a tuple of exception classes")
    return cast(ExceptionTypes, value)


@dataclass
class RetryConfig:
    """How one step retries a failed call, inside the job.

    Args:
        max_attempts: Calls in all, the first included (3 = up to 2 retries). 1 = no retry.
        min_wait_seconds: Shortest wait between attempts.
        max_wait_seconds: Longest wait; also caps a server's ``Retry-After``.
        multiplier: Wait before retry ``n`` is ``multiplier * 2 ** (n - 1)``, kept
            between the two bounds.
        retry_on: Errors that are retried (anywhere in the error's cause chain).
            ``None``: the step's defaults (see the module docstring).
        never_retry: Errors never retried, even when they match ``retry_on`` (wins).
            ``None``: the step's defaults.

    An error with a ``retry_after`` (seconds; an HTTP ``Retry-After``) waits that long
    instead, up to ``max_wait_seconds``.
    """

    max_attempts: int = 3
    min_wait_seconds: float = 1.0
    max_wait_seconds: float = 60.0
    multiplier: float = 2.0
    retry_on: Optional[ExceptionTypes] = None
    never_retry: Optional[ExceptionTypes] = None

    def __post_init__(self) -> None:
        attempts = cast(object, self.max_attempts)
        if isinstance(attempts, bool) or not isinstance(attempts, int):
            raise TypeError(f"RetryConfig.max_attempts must be an int, got {attempts!r}")
        if self.max_attempts < 1:
            raise ValueError(f"RetryConfig.max_attempts must be >= 1, got {self.max_attempts}")
        if self.min_wait_seconds < 0 or self.max_wait_seconds < 0 or self.multiplier < 0:
            raise ValueError("RetryConfig waits and multiplier must be >= 0")
        self.retry_on = _check_types("retry_on", self.retry_on)
        self.never_retry = _check_types("never_retry", self.never_retry)

    def should_retry(self, error: BaseException, defaults: RetryDefaults) -> bool:
        """Whether ``error`` may be retried (attempts left is the caller's check)."""
        if isinstance(error, (SkipJob, SkipRecord)):
            return False
        retry_on = self.retry_on if self.retry_on is not None else defaults[0]
        never_retry = self.never_retry if self.never_retry is not None else defaults[1]
        chain = error_chain(error)
        if any(isinstance(e, never_retry) for e in chain):
            return False
        return any(isinstance(e, retry_on) for e in chain)

    def wait_for(self, attempt: int, error: Optional[BaseException] = None) -> float:
        """Seconds to wait after failed attempt number ``attempt`` (1 for the first)."""
        for e in error_chain(error) if error is not None else []:
            retry_after = getattr(e, "retry_after", None)
            if isinstance(retry_after, (int, float)) and not isinstance(retry_after, bool):
                return min(max(float(retry_after), 0.0), self.max_wait_seconds)
        backoff = self.multiplier * (2 ** max(attempt - 1, 0))
        return float(max(self.min_wait_seconds, min(backoff, self.max_wait_seconds)))

    def to_dict(self) -> Dict[str, Any]:
        """JSON form, carried in a job message for a source (exception classes by path)."""
        return {
            "max_attempts": self.max_attempts,
            "min_wait_seconds": self.min_wait_seconds,
            "max_wait_seconds": self.max_wait_seconds,
            "multiplier": self.multiplier,
            "retry_on": None if self.retry_on is None else [_type_name(t) for t in self.retry_on],
            "never_retry": (
                None if self.never_retry is None else [_type_name(t) for t in self.never_retry]
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RetryConfig":
        """Rebuild from :meth:`to_dict`. A class the worker cannot import is left out."""

        def types(names: Any) -> Optional[ExceptionTypes]:
            if names is None:
                return None
            found = (_load_type(str(n)) for n in cast(List[Any], names))
            return tuple(t for t in found if t is not None)

        return cls(
            max_attempts=int(data.get("max_attempts", 3)),
            min_wait_seconds=float(data.get("min_wait_seconds", 1.0)),
            max_wait_seconds=float(data.get("max_wait_seconds", 60.0)),
            multiplier=float(data.get("multiplier", 2.0)),
            retry_on=types(data.get("retry_on")),
            never_retry=types(data.get("never_retry")),
        )


class _Counts:
    """Retries per step for one job; shared by its tasks and threads."""

    def __init__(self, into: Dict[str, int]):
        self.counts = into
        self.lock = threading.Lock()

    def add(self, step: str) -> None:
        with self.lock:
            self.counts[step] = self.counts.get(step, 0) + 1


_current_counts: ContextVar[Optional[_Counts]] = ContextVar("reflowfy_retry_counts", default=None)


@contextmanager
def count_retries(
    into: Optional[Dict[str, int]] = None,
) -> Generator[Dict[str, int], None, None]:
    """Count every step retry made inside this block (step name -> retries) in ``into``.

    Tasks and threads started inside the block copy the context, so a per-record
    step's concurrent records and ``asyncio.to_thread`` calls all count here.
    """
    counts = _Counts(into if into is not None else {})
    token = _current_counts.set(counts)
    try:
        yield counts.counts
    finally:
        _current_counts.reset(token)


def _next_wait(
    config: RetryConfig,
    defaults: RetryDefaults,
    step: str,
    attempt: int,
    error: Exception,
    log_level: int,
) -> Optional[float]:
    """Seconds to wait before retrying ``error``, or None to give up (and re-raise)."""
    if attempt >= config.max_attempts or not config.should_retry(error, defaults):
        return None
    wait = config.wait_for(attempt, error)
    counts = _current_counts.get()
    if counts is not None:
        counts.add(step)
    # The type only: a message can carry record values.
    logger.log(
        log_level,
        "%s failed (%s), retry %d/%d in %.1fs",
        step,
        type(error).__name__,
        attempt,
        config.max_attempts - 1,
        wait,
        extra={
            "event": "step_retry",
            "step": step,
            "attempt": attempt,
            "max_attempts": config.max_attempts,
            "error_type": type(error).__name__,
            "retry_in_seconds": round(wait, 3),
        },
    )
    return wait


async def retry_async(
    call: Callable[[], Awaitable[T]],
    config: Optional[RetryConfig],
    *,
    step: str,
    defaults: Callable[[], RetryDefaults] = step_defaults,
    log_level: int = logging.INFO,
) -> T:
    """Await ``call()``, calling it again per ``config`` when it raises. ``None``: once."""
    if config is None or config.max_attempts <= 1:
        return await call()
    resolved = defaults()
    attempt = 1
    while True:
        try:
            return await call()
        except Exception as exc:
            wait = _next_wait(config, resolved, step, attempt, exc, log_level)
            if wait is None:
                raise
        await asyncio.sleep(wait)
        attempt += 1


def retry_sync(
    call: Callable[[], T],
    config: Optional[RetryConfig],
    *,
    step: str,
    defaults: Callable[[], RetryDefaults] = step_defaults,
    log_level: int = logging.INFO,
) -> T:
    """:func:`retry_async` for code already in a worker thread (it sleeps the thread)."""
    if config is None or config.max_attempts <= 1:
        return call()
    resolved = defaults()
    attempt = 1
    while True:
        try:
            return call()
        except Exception as exc:
            wait = _next_wait(config, resolved, step, attempt, exc, log_level)
            if wait is None:
                raise
        time.sleep(wait)
        attempt += 1
