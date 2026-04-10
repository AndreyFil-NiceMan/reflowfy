"""Factory for creating source instances by type name."""

from typing import Any, Dict, Type, List
from reflowfy.sources.base import BaseSource


class SourceFactory:
    """
    Factory for creating sources by type name.

    Allows dynamic source creation based on configuration:

    Example:
        >>> source = SourceFactory.create("elastic", {
        ...     "url": "http://localhost:9200",
        ...     "index": "my-index"
        ... })
    """

    _registry: Dict[str, Type[BaseSource]] = {}

    @classmethod
    def register(cls, type_name: str, source_class: Type[BaseSource]) -> None:
        """
        Register a source type.

        Args:
            type_name: Short name for the source type (e.g., "elastic")
            source_class: The source class to register
        """
        cls._registry[type_name] = source_class

    @classmethod
    def create(cls, type_name: str, config: Dict[str, Any]) -> BaseSource:
        """
        Create a source instance by type name.

        Args:
            type_name: Registered source type name
            config: Configuration to pass to the source

        Returns:
            Configured BaseSource instance

        Raises:
            ValueError: If type_name is not registered
        """
        if type_name not in cls._registry:
            available = ", ".join(cls._registry.keys()) if cls._registry else "none"
            raise ValueError(
                f"Unknown source type: '{type_name}'. Available types: {available}"
            )
        return cls._registry[type_name](config)

    @classmethod
    def list_types(cls) -> List[str]:
        """List all registered source types."""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, type_name: str) -> bool:
        """Check if a source type is registered."""
        return type_name in cls._registry


def _register_builtin_sources() -> None:
    """Register built-in source types."""
    try:
        from reflowfy.sources.elastic import ElasticSource
        SourceFactory.register("elastic", ElasticSource)
    except ImportError:
        pass

    try:
        from reflowfy.sources.sql import SQLSource
        SourceFactory.register("sql", SQLSource)
    except ImportError:
        pass

    try:
        from reflowfy.sources.mock import MockSource
        SourceFactory.register("mock", MockSource)
    except ImportError:
        pass


# Auto-register on import
_register_builtin_sources()
