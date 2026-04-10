"""Decorator for registering reusable source configurations.

Users define reusable source factory functions with the @source decorator:

Example:
    @source("production_elastic")
    def production_elastic(**overrides):
        return elastic_source(
            url=os.getenv("ELASTIC_URL", "http://elasticsearch:9200"),
            index=overrides.get("index", "logs-*"),
            scroll="2m",
            size=1000,
        )

    # Then in a pipeline:
    def define_source(self, params):
        return production_elastic(index="my-specific-index")
"""

from typing import Callable, Dict, Optional, TypeVar

F = TypeVar("F", bound=Callable)


class SourceRegistry:
    """Registry for reusable source factory functions."""

    _sources: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str, factory: Callable) -> None:
        """Register a source factory function."""
        cls._sources[name] = factory
        print(f"✓ Registered reusable source: {name}")

    @classmethod
    def get(cls, name: str) -> Optional[Callable]:
        """Get a source factory by name."""
        return cls._sources.get(name)

    @classmethod
    def list_all(cls) -> list:
        """List all registered source names."""
        return list(cls._sources.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registered sources (for testing)."""
        cls._sources.clear()


source_registry = SourceRegistry()


def source(name: str) -> Callable[[F], F]:
    """
    Decorator to register a reusable source configuration.

    Args:
        name: Unique name for this source configuration

    Returns:
        Decorator that registers the function and returns it unchanged

    Example:
        @source("production_elastic")
        def production_elastic(**overrides):
            return elastic_source(url="http://prod:9200", **overrides)
    """
    def decorator(func: F) -> F:
        source_registry.register(name, func)
        return func
    return decorator
