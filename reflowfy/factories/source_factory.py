"""Factory for creating source instances by type name.

Every concrete :class:`~reflowfy.sources.base.BaseSource` subclass registers itself here
when its class is defined (``BaseSource.__init_subclass__``), so a source written in a
user project is rebuilt on the worker the same way a built-in one is: discovery imports
the module on every service, and the worker looks the job's ``source.type`` up here.
"""

import json
import logging
import threading
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Type

from reflowfy.core.secrets import (
    decode_secret_refs,
    encode_secret_refs,
    literal_secret_fields,
    redact_config,
)
from reflowfy.sources.base import BaseSource

logger = logging.getLogger(__name__)


class SourceRoundTripError(ValueError):
    """A source cannot be rebuilt from its serialized config (found at planning)."""


class SourceFactory:
    """
    Factory for creating sources by type name.

    Allows dynamic source creation based on configuration:

    Example:
        >>> source = SourceFactory.create("ElasticSource", {
        ...     "url": "http://localhost:9200",
        ...     "index": "my-index"
        ... })
    """

    _registry: Dict[str, Type[BaseSource]] = {}
    # (type, config keys) shapes already proven to round-trip in this process.
    _checked: Set[Tuple[str, FrozenSet[str]]] = set()
    # (pipeline, type, field) already warned about holding a literal secret.
    _warned_literal: Set[Tuple[Optional[str], str, str]] = set()
    _lock = threading.Lock()

    @classmethod
    def register(cls, type_name: str, source_class: Type[BaseSource]) -> None:
        """
        Register a source type.

        Args:
            type_name: Class name used as the registry key (e.g., "ElasticSource")
            source_class: The source class to register
        """
        previous = cls._registry.get(type_name)
        if previous is not None and previous is not source_class:
            same_place = (previous.__module__, previous.__qualname__) == (
                source_class.__module__,
                source_class.__qualname__,
            )
            if not same_place:  # a module re-imported redefines the same class
                logger.warning(
                    "Source type %r registered twice (%s.%s replaces %s.%s): jobs of "
                    "either rebuild as the later one; give one a different class name",
                    type_name,
                    source_class.__module__,
                    source_class.__qualname__,
                    previous.__module__,
                    previous.__qualname__,
                    extra={"event": "source_type_replaced", "source_type": type_name},
                )
        cls._registry[type_name] = source_class

    @classmethod
    def create(cls, type_name: str, config: Dict[str, Any]) -> BaseSource:
        """Reconstruct a source from its registry type name and config dict.

        ``secret()`` references in the config are read from this process's
        environment, then the class's ``from_config`` builds the instance (by default
        ``cls(**config)``).
        """
        if type_name not in cls._registry:
            available = ", ".join(sorted(cls._registry)) if cls._registry else "none"
            raise ValueError(
                f"Unknown source type: '{type_name}'. Available types: {available}. "
                "A custom source must be defined in a module pipeline discovery imports "
                "(the pipelines/ or sources/ directory) on every service."
            )
        return cls._registry[type_name].from_config(decode_secret_refs(config))

    @classmethod
    def serialize(cls, source: BaseSource, pipeline_name: Optional[str] = None) -> Dict[str, Any]:
        """Serialize a source instance to a ``{type, config}`` descriptor.

        ``secret()`` values are written as their variable name. The first time a
        source type with a given set of config keys is serialized, it is also rebuilt
        from the descriptor (:meth:`check_round_trip`), so a source the worker could
        not rebuild fails here, at planning, instead of on every job.
        """
        descriptor: Dict[str, Any] = {
            "type": source.registry_type,
            "config": encode_secret_refs(source.config),
        }
        cls.check_round_trip(source, descriptor)
        cls._warn_literal_secrets(source, descriptor, pipeline_name)
        return descriptor

    @classmethod
    def check_round_trip(cls, source: BaseSource, descriptor: Dict[str, Any]) -> None:
        """Rebuild ``source`` from ``descriptor`` as a worker would; raise if it cannot."""
        type_name: str = descriptor["type"]
        config: Dict[str, Any] = descriptor["config"]
        shape = (type_name, frozenset(config))
        if shape in cls._checked:
            return
        name = type(source).__name__
        fix = (
            f"{name}.config must hold exactly its constructor's keyword arguments, "
            f"or {name} must override the classmethod from_config(cls, config)"
        )
        try:
            wire = json.loads(json.dumps(config))
        except (TypeError, ValueError) as exc:
            raise SourceRoundTripError(
                f"Source {name} cannot be sent to a worker: its config is not JSON "
                f"({exc}). Keep only JSON values (str, numbers, bool, None, lists, "
                "dicts) in config."
            ) from exc
        registered = cls._registry.get(type_name)
        if registered is not type(source):
            raise SourceRoundTripError(
                f"Source {name} cannot be rebuilt on a worker: its type {type_name!r} "
                + (
                    "is not registered"
                    if registered is None
                    else f"is registered to {registered.__module__}.{registered.__qualname__}"
                )
                + ". Call SourceFactory.register(type_name, cls) for a source whose "
                "registry_type is not its class name."
            )
        try:
            rebuilt = cls.create(type_name, wire)
        except Exception as exc:
            raise SourceRoundTripError(
                f"Source {name} cannot be rebuilt on a worker from its config "
                f"({type(exc).__name__}: {exc}). {fix}."
            ) from exc
        rebuilt_config = json.loads(json.dumps(encode_secret_refs(rebuilt.config)))
        if rebuilt_config != wire:
            changed = sorted(
                k
                for k in set(wire) | set(rebuilt_config)
                if wire.get(k, ...) != rebuilt_config.get(k, ...)
            )
            raise SourceRoundTripError(
                f"Source {name} does not round-trip: rebuilt from its config, it has a "
                f"different config (keys {', '.join(changed)}). {fix}."
            )
        with cls._lock:
            cls._checked.add(shape)

    @classmethod
    def _warn_literal_secrets(
        cls, source: BaseSource, descriptor: Dict[str, Any], pipeline_name: Optional[str]
    ) -> None:
        """Warn, once per pipeline/type/field, that a literal credential is serialized."""
        type_name: str = descriptor["type"]
        for field in literal_secret_fields(descriptor["config"], source.secret_fields):
            key = (pipeline_name, type_name, field)
            with cls._lock:
                if key in cls._warned_literal:
                    continue
                cls._warned_literal.add(key)
            logger.warning(
                "Source %s field %r holds a literal credential, which is stored in every "
                "job row and Kafka message of this pipeline; pass it as "
                'secret("ENV_NAME") so only the variable name is sent',
                type_name,
                field,
                extra={
                    "event": "literal_secret_serialized",
                    "source_type": type_name,
                    "field": field,
                    "pipeline_name": pipeline_name,
                },
            )

    @classmethod
    def redact(cls, descriptor: Dict[str, Any]) -> Dict[str, Any]:
        """Copy of a ``{type, config}`` descriptor with the source's credentials hidden."""
        source_class = cls._registry.get(str(descriptor.get("type")))
        fields = source_class.secret_fields if source_class is not None else ()
        redacted: Dict[str, Any] = {k: redact_config(v) for k, v in descriptor.items()}
        redacted["config"] = redact_config(descriptor.get("config"), fields)
        return redacted

    @classmethod
    def list_types(cls) -> List[str]:
        """List all registered source types."""
        return list(cls._registry.keys())

    @classmethod
    def is_registered(cls, type_name: str) -> bool:
        """Check if a source type is registered."""
        return type_name in cls._registry


def _register_builtin_sources() -> None:
    """Import the built-in sources: each registers itself when its class is defined."""
    import reflowfy.sources.mock  # noqa: F401  # pyright: ignore[reportUnusedImport]
    import reflowfy.sources.static  # noqa: F401  # pyright: ignore[reportUnusedImport]

    for module in (
        "reflowfy.sources.elastic",
        "reflowfy.sources.sql",
        "reflowfy.sources.s3",
        "reflowfy.sources.api",
    ):
        try:
            __import__(module)
        except ImportError:
            pass  # optional dependency (boto3, elasticsearch) not installed


# Auto-register on import
_register_builtin_sources()
