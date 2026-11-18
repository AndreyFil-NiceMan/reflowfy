"""``@destination(per_record=True)``: choose each record's destination with a function.

Example:
    @destination(per_record=True, on_error="skip")
    def by_engine(record, runtime_params):
        return kafka_destination(bootstrap_servers=..., topic=f"stt-{record['engine']}")

    def define_destination(self, records, runtime_params):
        return by_engine            # or by_engine(option=...) when it takes options

A reusable destination *configuration* needs no decorator: a plain function that
returns ``kafka_destination(...)`` does it. ``@destination("name")`` without
``per_record`` (which registered such a function in a registry nothing reads) and
``@record_destination`` (the old spelling of ``per_record=True``) are deprecated.
"""

import inspect
import logging
import warnings
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, TypeVar, overload

if TYPE_CHECKING:
    from reflowfy.destinations.record import OnError, RecordDestination, RecordDestinationFn

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class DestinationRegistry:
    """Registry of destination factories by name. Deprecated: nothing in the framework reads it."""

    _destinations: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[..., Any]) -> None:
        """Register a destination factory function."""
        cls._destinations[name] = factory
        logger.debug(
            "Registered reusable destination: %s", name, extra={"event": "destination_registered"}
        )

    @classmethod
    def get(cls, name: str) -> Optional[Callable[..., Any]]:
        """Get a destination factory by name."""
        return cls._destinations.get(name)

    @classmethod
    def list_all(cls) -> List[str]:
        """List all registered destination names."""
        return list(cls._destinations.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered destinations (for testing)."""
        cls._destinations.clear()


destination_registry = DestinationRegistry()


@overload
def destination(name: str, /) -> Callable[[F], F]: ...


@overload
def destination(
    name: Optional[str] = None,
    *,
    per_record: Literal[True],
    on_error: "OnError" = "fail",
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Callable[["RecordDestinationFn"], "type[RecordDestination]"]: ...


def destination(
    name: Any = None,
    *,
    per_record: Optional[bool] = None,
    on_error: "OnError" = "fail",
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Any:
    """
    Decorator for a destination chosen per record (``per_record=True``).

    The function is called once per record with ``(record, runtime_params, **options)``
    and returns the destination that record goes to — a ``kafka_destination(...)`` with
    its own cluster, topic and credentials, an ``api_destination(...)``, any destination,
    or a list / ``fan_out(...)`` of them. Records whose destinations have the same class
    and config share one, and those destinations are sent to concurrently. It may be
    ``async def`` (a lookup in a service, say), and then ``concurrency`` records are
    routed at once — the same knobs as ``@transformation(per_record=True)``.

    The decorated name becomes a destination class: return it from
    ``define_destination``, bare or called with its options.

    Args:
        name: Reported as the step that routed and skipped records; defaults to the
            function's name.
        per_record: Must be ``True``. (Without it, ``@destination("name")`` is the
            deprecated registry of destination factories: use a plain function.)
        on_error: ``"fail"`` (default) fails the job when the function raises on a
            record, naming the record's position; ``"skip"`` skips that record with
            the exception's type name as the reason. ``raise SkipRecord("why")``
            skips a record either way.
        concurrency: Requests/destinations sent to at once, and records routed at once
            by an ``async def`` function. Defaults to ``TRANSFORM_CONCURRENCY`` (16).
        timeout: ``async def`` only: seconds allowed per record; ``None`` means no
            limit. A timeout counts as the function raising (``on_error``).

    Example:
        @destination(per_record=True, on_error="skip")
        def stt_engines(record, runtime_params):
            topic = Settings.KAFKA.topics[record["engine"]][runtime_params["env"]]
            return kafka_destination(bootstrap_servers=..., topic=topic.name)

        def define_destination(self, records, runtime_params):
            return stt_engines
    """
    if callable(name) and not isinstance(name, str):
        raise TypeError(
            "@destination needs per_record=True: @destination(per_record=True) routes "
            "each record. A reusable destination configuration is a plain function "
            "returning kafka_destination(...) — no decorator."
        )
    if name is not None and not isinstance(name, str):
        raise TypeError(f"destination: name must be a string, got {type(name).__name__}")
    if per_record is None and isinstance(name, str):
        return _legacy_destination(name, on_error, concurrency, timeout)
    if not per_record:
        raise TypeError(
            "@destination only exists per record: pass per_record=True. A reusable "
            "destination configuration is a plain function returning "
            "kafka_destination(...) — no decorator."
        )
    owner = f"destination {name!r}" if name else "destination"
    _check_route_knobs(owner, on_error, concurrency, timeout)

    def decorator(func: "RecordDestinationFn") -> "type[RecordDestination]":
        return _record_destination_class(
            func, name or func.__name__, on_error, concurrency, timeout
        )

    return decorator


def _legacy_destination(
    name: str, on_error: object, concurrency: Optional[int], timeout: Optional[float]
) -> Callable[[F], F]:
    if (on_error, concurrency, timeout) != ("fail", None, None):
        raise TypeError(
            f"destination {name!r}: on_error, concurrency and timeout need per_record=True"
        )
    warnings.warn(
        f"@destination({name!r}) without per_record is deprecated: it only registered the "
        "function in a registry nothing reads. Drop the decorator — a plain function "
        "returning a destination does the same.",
        DeprecationWarning,
        stacklevel=3,
    )

    def decorator(func: F) -> F:
        destination_registry.register(name, func)
        return func

    return decorator


def record_destination(
    name: str,
    on_error: "OnError" = "fail",
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Callable[["RecordDestinationFn"], "type[RecordDestination]"]:
    """Deprecated: ``@destination(name, per_record=True, ...)`` does the same.

    Kept so existing pipelines run unchanged; it warns (``DeprecationWarning``) where
    it is used.
    """
    warnings.warn(
        "@record_destination is deprecated: use "
        f"@destination({name!r}, per_record=True, ...) instead",
        DeprecationWarning,
        stacklevel=2,
    )
    _check_route_knobs(f"record_destination {name!r}", on_error, concurrency, timeout)

    def decorator(func: "RecordDestinationFn") -> "type[RecordDestination]":
        return _record_destination_class(func, name, on_error, concurrency, timeout)

    return decorator


def _check_route_knobs(
    owner: str, on_error: object, concurrency: Optional[int], timeout: Optional[float]
) -> None:
    from reflowfy.destinations.base import check_concurrency

    if on_error not in ("fail", "skip"):
        raise ValueError(f"{owner}: on_error must be 'fail' or 'skip', got {on_error!r}")
    if concurrency is not None:
        check_concurrency(owner, concurrency)
    if timeout is not None and timeout <= 0:
        raise ValueError(f"{owner}: timeout must be > 0")


def _record_destination_class(
    func: "RecordDestinationFn",
    name: str,
    on_error: "OnError",
    concurrency: Optional[int],
    timeout: Optional[float],
) -> "type[RecordDestination]":
    from reflowfy.destinations.record import make_record_destination

    if timeout is not None and not inspect.iscoroutinefunction(func):
        # A thread running sync code cannot be stopped, so the limit would be a lie.
        raise TypeError(
            f"destination {name!r}: timeout needs an `async def` function; "
            "a running sync function cannot be interrupted"
        )
    cls = make_record_destination(func, name, on_error, concurrency, timeout)
    destination_registry.register(name, cls)
    return cls
