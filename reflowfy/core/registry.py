"""Pipeline registry for dynamic registration and lookup."""

import logging
import threading
from typing import TYPE_CHECKING, Dict, List, Optional

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from reflowfy.core.abstract_pipeline import AbstractPipeline


class PipelineRegistry:
    """
    Thread-safe singleton registry for pipelines.

    Pipelines are registered at module import time via:
        pipeline_registry.register(pipeline)

    The API uses this registry to:
    - Discover all registered pipelines
    - Generate dynamic routes
    - Look up pipelines for execution
    """

    _instance: "Optional[PipelineRegistry]" = None
    _lock = threading.Lock()

    _pipelines: Dict[str, "AbstractPipeline"]
    _failures: Dict[str, str]
    _registry_lock: threading.RLock

    def __new__(cls) -> "PipelineRegistry":
        """Singleton pattern to ensure single registry instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pipelines = {}
                    cls._instance._failures = {}
                    cls._instance._registry_lock = threading.RLock()
        return cls._instance

    def record_failure(self, name: str, error: Exception) -> None:
        """Record why a pipeline could not be registered, for later lookups.

        Args:
            name: Pipeline name that failed to register
            error: The exception raised during instantiation/registration
        """
        with self._registry_lock:
            self._failures[name] = f"{type(error).__name__}: {error}"

    def describe_missing(self, name: str) -> str:
        """Explain why a pipeline is absent, as a message suffix.

        Returns:
            e.g. " (it failed to register: KeyError: 'ES_URL')", or "" if the
            name was never attempted — then it's a typo or an undiscovered module.
        """
        with self._registry_lock:
            reason = self._failures.get(name)
        return f" (it failed to register: {reason})" if reason else ""

    def register(self, pipeline: "AbstractPipeline") -> None:
        """
        Register a pipeline.

        Args:
            pipeline: AbstractPipeline instance to register

        Note:
            If a pipeline with the same name is already registered,
            registration is silently skipped (idempotent).
        """
        with self._registry_lock:
            if pipeline.name in self._pipelines:
                # Idempotent: skip duplicate registration silently
                return

            self._pipelines[pipeline.name] = pipeline
            logger.debug("Registered pipeline: %s", pipeline.name)

    def get(self, name: str) -> Optional["AbstractPipeline"]:
        """
        Retrieve a pipeline by name.

        Args:
            name: Pipeline name

        Returns:
            AbstractPipeline instance or None if not found
        """
        with self._registry_lock:
            return self._pipelines.get(name)

    def list_all(self) -> List["AbstractPipeline"]:
        """
        Get all registered pipelines.

        Returns:
            List of all AbstractPipeline instances
        """
        with self._registry_lock:
            return list(self._pipelines.values())

    def list_names(self) -> List[str]:
        """
        Get all registered pipeline names.

        Returns:
            List of pipeline names
        """
        with self._registry_lock:
            return list(self._pipelines.keys())

    def exists(self, name: str) -> bool:
        """
        Check if a pipeline is registered.

        Args:
            name: Pipeline name

        Returns:
            True if pipeline exists, False otherwise
        """
        with self._registry_lock:
            return name in self._pipelines

    def unregister(self, name: str) -> bool:
        """
        Unregister a pipeline (mainly for testing).

        Args:
            name: Pipeline name to remove

        Returns:
            True if pipeline was removed, False if not found
        """
        with self._registry_lock:
            if name in self._pipelines:
                del self._pipelines[name]
                return True
            return False

    def clear(self) -> None:
        """Clear all registered pipelines (mainly for testing)."""
        with self._registry_lock:
            self._pipelines.clear()
            self._failures.clear()


# Global singleton instance
pipeline_registry = PipelineRegistry()
