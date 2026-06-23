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
        """Reconstruct a source from its registry type name and config dict.

        Every built-in source stores ``config`` as exactly its constructor
        kwargs, so reconstruction is a uniform ``cls(**config)``.
        """
        if type_name not in cls._registry:
            available = ", ".join(sorted(cls._registry)) if cls._registry else "none"
            raise ValueError(
                f"Unknown source type: '{type_name}'. Available types: {available}"
            )
        return cls._registry[type_name](**config)

    @classmethod
    def serialize(cls, source: BaseSource) -> Dict[str, Any]:
        """Serialize a source instance to a ``{type, config}`` descriptor."""
        return {"type": source.registry_type, "config": source.config}

    @classmethod
    def list_types(cls) -> List[str]:
        """List all registered source types."""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, type_name: str) -> bool:
        """Check if a source type is registered."""
        return type_name in cls._registry


def _register_builtin_sources() -> None:
    """Register built-in source types by class name."""
    from reflowfy.sources.static import StaticSource
    from reflowfy.sources.mock import MockSource

    SourceFactory.register("StaticSource", StaticSource)
    SourceFactory.register("MockSource", MockSource)

    for module, classname in (
        ("reflowfy.sources.elastic", "ElasticSource"),
        ("reflowfy.sources.sql", "SqlSource"),
        ("reflowfy.sources.s3", "S3Source"),
        ("reflowfy.sources.api", "IDBasedAPISource"),
    ):
        try:
            mod = __import__(module, fromlist=[classname])
            SourceFactory.register(classname, getattr(mod, classname))
        except ImportError:
            pass  # optional dependency (boto3, elasticsearch) not installed


# Auto-register on import
_register_builtin_sources()
