"""Pipeline registry for dynamic registration and lookup."""

import threading
from typing import Dict, List, Optional
from reflowfy.core.pipeline import Pipeline


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
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single registry instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pipelines: Dict[str, Pipeline] = {}
                    cls._instance._registry_lock = threading.RLock()
        return cls._instance
    
    def register(self, pipeline: Pipeline) -> None:
        """
        Register a pipeline.
        
        Args:
            pipeline: Pipeline instance to register
        
        Raises:
            ValueError: If pipeline with same name already registered
        """
        with self._registry_lock:
            if pipeline.name in self._pipelines:
                raise ValueError(
                    f"Pipeline '{pipeline.name}' is already registered. "
                    "Each pipeline must have a unique name."
                )
            
            self._pipelines[pipeline.name] = pipeline
            print(f"✓ Registered pipeline: {pipeline.name}")
    
    def get(self, name: str) -> Optional[Pipeline]:
        """
        Retrieve a pipeline by name.
        
        Args:
            name: Pipeline name
        
        Returns:
            Pipeline instance or None if not found
        """
        with self._registry_lock:
            return self._pipelines.get(name)
    
    def list_all(self) -> List[Pipeline]:
        """
        Get all registered pipelines.
        
        Returns:
            List of all Pipeline instances
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


# Global singleton instance
pipeline_registry = PipelineRegistry()
