"""``@destination(per_record=True)``: choose each record's destination with a function.

Example:
    @destination(per_record=True, on_error="skip")
    def by_region(record, runtime_params):
        return kafka_destination(bootstrap_servers=..., topic=f"orders-{record['region']}")

    def define_destination(self, records, runtime_params):
        return by_region            # or by_region(option=...) when it takes options

A reusable destination *configuration* needs no decorator: a plain function that
returns ``kafka_destination(...)`` does it.
"""

import inspect
from typing import TYPE_CHECKING, Callable, Optional, cast

if TYPE_CHECKING:
    from reflowfy.destinations.record import OnError, RecordDestination, RecordDestinationFn


def destination(
    name: Optional[str] = None,
    *,
    per_record: bool = False,
    on_error: "OnError" = "fail",
    concurrency: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Callable[["RecordDestinationFn"], "type[RecordDestination]"]:
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
        per_record: Must be ``True``: a destination decorator only exists per record.
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
        def by_region(record, runtime_params):
            return kafka_destination(bootstrap_servers=..., topic=f"orders-{record['region']}")

        def define_destination(self, records, runtime_params):
            return by_region
    """
    raw_name = cast(object, name)  # checked at run time: a bare `@destination` passes the function
    if callable(raw_name) and not isinstance(raw_name, str):
        raise TypeError(
            "@destination needs per_record=True: @destination(per_record=True) routes "
            "each record. A reusable destination configuration is a plain function "
            "returning kafka_destination(...) — no decorator."
        )
    if raw_name is not None and not isinstance(raw_name, str):
        raise TypeError(f"destination: name must be a string, got {type(raw_name).__name__}")
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
    return make_record_destination(func, name, on_error, concurrency, timeout)
