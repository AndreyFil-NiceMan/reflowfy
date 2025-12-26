"""Registry for transformation lookup by name."""

import threading
from typing import Dict, List, Optional, Type


class TransformationRegistry:
    """
    Thread-safe singleton registry for transformations.
    
    Transformations are automatically registered via metaclass when defined.
    Workers use this registry to load transformations by name from job metadata.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single registry instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._transformations: Dict[str, Type] = {}
                    cls._instance._registry_lock = threading.RLock()
        return cls._instance
    
    def register(self, transformation_class: Type) -> None:
        """
        Register a transformation class.
        
        Args:
            transformation_class: Transformation class to register
        
        Raises:
            ValueError: If transformation with same name already registered
        """
        with self._registry_lock:
            name = transformation_class.name
            
            if not name:
                raise ValueError(
                    f"Transformation class {transformation_class.__name__} "
                    "must define a non-empty 'name' attribute"
                )
            
            if name in self._transformations:
                existing = self._transformations[name]
                raise ValueError(
                    f"Transformation '{name}' is already registered by "
                    f"{existing.__name__}. Cannot register {transformation_class.__name__}."
                )
            
            self._transformations[name] = transformation_class
            print(f"✓ Registered transformation: {name}")
    
    def get(self, name: str) -> Optional[Type]:
        """
        Get transformation class by name.
        
        Args:
            name: Transformation name
        
        Returns:
            Transformation class or None if not found
        """
        with self._registry_lock:
            return self._transformations.get(name)
    
    def create_instance(self, name: str):
        """
        Create a new instance of a transformation by name.
        
        Args:
            name: Transformation name
        
        Returns:
            New transformation instance
        
        Raises:
            ValueError: If transformation not found
        """
        with self._registry_lock:
            transformation_class = self._transformations.get(name)
            
            if transformation_class is None:
                raise ValueError(
                    f"Transformation '{name}' not found. "
                    f"Available: {list(self._transformations.keys())}"
                )
            
            return transformation_class()
    
    def list_all(self) -> List[str]:
        """
        Get all registered transformation names.
        
        Returns:
            List of transformation names
        """
        with self._registry_lock:
            return list(self._transformations.keys())
    
    def exists(self, name: str) -> bool:
        """
        Check if a transformation is registered.
        
        Args:
            name: Transformation name
        
        Returns:
            True if transformation exists, False otherwise
        """
        with self._registry_lock:
            return name in self._transformations
    
    def clear(self) -> None:
        """Clear all registered transformations (mainly for testing)."""
        with self._registry_lock:
            self._transformations.clear()


# Global singleton instance
transformation_registry = TransformationRegistry()
