"""Factory for creating destination instances by type name."""

from typing import Any, Dict, Type, List
from reflowfy.destinations.base import BaseDestination


class DestinationFactory:
    """
    Factory for creating destinations by type name.

    Allows dynamic destination creation based on configuration:

    Example:
        >>> dest = DestinationFactory.create("kafka", {
        ...     "bootstrap_servers": "localhost:9092",
        ...     "topic": "output-topic"
        ... })
    """

    _registry: Dict[str, Type[BaseDestination]] = {}

    @classmethod
    def register(cls, type_name: str, dest_class: Type[BaseDestination]) -> None:
        """
        Register a destination type.

        Args:
            type_name: Short name for the destination type (e.g., "kafka")
            dest_class: The destination class to register
        """
        cls._registry[type_name] = dest_class

    @classmethod
    def create(cls, type_name: str, config: Dict[str, Any]) -> BaseDestination:
        """
        Create a destination instance by type name.

        Args:
            type_name: Registered destination type name
            config: Configuration to pass to the destination

        Returns:
            Configured BaseDestination instance

        Raises:
            ValueError: If type_name is not registered
        """
        if type_name not in cls._registry:
            available = ", ".join(cls._registry.keys()) if cls._registry else "none"
            raise ValueError(
                f"Unknown destination type: '{type_name}'. Available types: {available}"
            )
        return cls._registry[type_name](config)

    @classmethod
    def list_types(cls) -> List[str]:
        """List all registered destination types."""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, type_name: str) -> bool:
        """Check if a destination type is registered."""
        return type_name in cls._registry


def _register_builtin_destinations() -> None:
    """Register built-in destination types."""
    try:
        from reflowfy.destinations.kafka import KafkaDestination
        DestinationFactory.register("kafka", KafkaDestination)
    except ImportError:
        pass

    try:
        from reflowfy.destinations.api import ApiDestination
        DestinationFactory.register("api", ApiDestination)
    except ImportError:
        pass

    try:
        from reflowfy.destinations.console import ConsoleDestination
        DestinationFactory.register("console", ConsoleDestination)
    except ImportError:
        pass


# Auto-register on import
_register_builtin_destinations()
