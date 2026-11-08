"""Decorator for registering reusable destination configurations.

Users define reusable destination factory functions with the @destination decorator:

Example:
    @destination("production_kafka")
    def production_kafka(**overrides):
        return kafka_destination(
            bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:9092"),
            topic=overrides.get("topic", "default-topic"),
            compression_type="gzip",
        )

    # Then in a pipeline:
    def define_destination(self, records, runtime_params):
        return production_kafka(topic="my-output")
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, TypeVar

if TYPE_CHECKING:
    from reflowfy.destinations.record import OnError, RecordDestination, RecordDestinationFn

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class DestinationRegistry:
    """Registry for reusable destination factory functions."""

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


def destination(name: str) -> Callable[[F], F]:
    """
    Decorator to register a reusable destination configuration.

    Args:
        name: Unique name for this destination configuration

    Returns:
        Decorator that registers the function and returns it unchanged

    Example:
        @destination("production_kafka")
        def production_kafka(**overrides):
            return kafka_destination(bootstrap_servers="kafka:9092", **overrides)
    """

    def decorator(func: F) -> F:
        destination_registry.register(name, func)
        return func

    return decorator


def record_destination(
    name: str,
    on_error: "OnError" = "fail",
    concurrency: Optional[int] = None,
) -> Callable[["RecordDestinationFn"], "type[RecordDestination]"]:
    """
    Decorator for a destination chosen per record.

    The function is called once per record with ``(record, runtime_params)`` and
    returns the destination that record goes to — a ``kafka_destination(...)`` with
    its own cluster, topic and credentials, an ``api_destination(...)``, any
    destination. Records whose destinations have the same class and config share one.
    It may be ``async def``; ``concurrency`` records are then routed at once.

    The decorated name becomes a destination class: return an instance of it from
    ``define_destination``. Keyword arguments given there are passed on to every call.

    Args:
        name: Unique name; reported as the step that skipped records.
        on_error: ``"fail"`` (default) fails the job when the function raises on a
            record, naming the record's position; ``"skip"`` skips that record with
            the exception's type name as the reason. ``raise SkipRecord("why")``
            skips a record either way.
        concurrency: Records routed at once by an ``async def`` function. Defaults to
            the ``TRANSFORM_CONCURRENCY`` env var (16).

    Example:
        @record_destination("stt_engines", on_error="skip")
        def stt_engines(record, runtime_params):
            topic = Settings.KAFKA.topics[record["engine"]][runtime_params["env"]]
            return kafka_destination(bootstrap_servers=..., topic=topic.name)

        def define_destination(self, records, runtime_params):
            return stt_engines()
    """
    from reflowfy.destinations.base import check_concurrency
    from reflowfy.destinations.record import make_record_destination

    if on_error not in ("fail", "skip"):
        raise ValueError(f"record_destination {name!r}: on_error must be 'fail' or 'skip'")
    if concurrency is not None:
        check_concurrency(f"record_destination {name!r}", concurrency)

    def decorator(func: "RecordDestinationFn") -> "type[RecordDestination]":
        cls = make_record_destination(func, name, on_error, concurrency)
        destination_registry.register(name, cls)
        return cls

    return decorator
