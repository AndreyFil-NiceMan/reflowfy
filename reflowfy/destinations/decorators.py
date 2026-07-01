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

from typing import Any, Callable, Dict, List, Optional, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


class DestinationRegistry:
    """Registry for reusable destination factory functions."""

    _destinations: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[..., Any]) -> None:
        """Register a destination factory function."""
        cls._destinations[name] = factory
        print(f"✓ Registered reusable destination: {name}")

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
