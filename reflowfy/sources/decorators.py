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
    def define_source(self, runtime_params):
        return production_elastic(index="my-specific-index")
"""

import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class SourceRegistry:
    """Registry for reusable source factory functions."""

    _sources: Dict[str, Callable[..., Any]] = {}

    @classmethod
    def register(cls, name: str, factory: Callable[..., Any]) -> None:
        """Register a source factory function."""
        cls._sources[name] = factory
        logger.debug("Registered reusable source: %s", name)

    @classmethod
    def get(cls, name: str) -> Optional[Callable[..., Any]]:
        """Get a source factory by name."""
        return cls._sources.get(name)

    @classmethod
    def list_all(cls) -> List[str]:
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
