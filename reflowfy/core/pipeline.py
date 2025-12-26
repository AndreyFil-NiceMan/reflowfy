"""Pipeline definition and builder."""

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import re


@dataclass
class Pipeline:
    """
    Represents a data pipeline with source, transformations, and destination.
    
    Attributes:
        name: Unique pipeline identifier
        source: Source configuration object
        transformations: List of transformation instances
        destination: Destination configuration object
        rate_limit: Optional rate limiting config (e.g., {"jobs_per_second": 50})
        config: Additional pipeline-specific configuration
    """
    
    name: str
    source: Any
    transformations: List[Any]
    destination: Any
    rate_limit: Optional[Dict[str, int]] = None
    config: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate pipeline configuration."""
        if not self.name:
            raise ValueError("Pipeline name cannot be empty")
        
        # Validate name format (alphanumeric, underscores, hyphens)
        if not re.match(r'^[a-zA-Z0-9_-]+$', self.name):
            raise ValueError(
                f"Pipeline name '{self.name}' must contain only alphanumeric "
                "characters, underscores, or hyphens"
            )
        
        if not self.source:
            raise ValueError("Pipeline source cannot be None")
        
        if not self.destination:
            raise ValueError("Pipeline destination cannot be None")
        
        if self.transformations is None:
            self.transformations = []
    
    def get_transformation_names(self) -> List[str]:
        """Return list of transformation names for job metadata."""
        return [t.name for t in self.transformations]
    
    def get_runtime_parameters(self) -> List[str]:
        """Extract runtime parameter names from source configuration."""
        if hasattr(self.source, 'get_runtime_parameters'):
            return self.source.get_runtime_parameters()
        return []
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata (excludes actual source/destination objects)."""
        return {
            "name": self.name,
            "transformations": self.get_transformation_names(),
            "rate_limit": self.rate_limit,
            "config": self.config,
            "runtime_parameters": self.get_runtime_parameters(),
        }


def build_pipeline(
    name: str,
    source: Any,
    transformations: List[Any],
    destination: Any,
    rate_limit: Optional[Dict[str, int]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Pipeline:
    """
    Factory function to build and validate a pipeline.
    
    Args:
        name: Unique pipeline name
        source: Source instance (e.g., elastic_source(...))
        transformations: List of transformation instances
        destination: Destination instance (e.g., kafka_destination(...))
        rate_limit: Optional rate limiting (e.g., {"jobs_per_second": 50})
        config: Additional configuration
    
    Returns:
        Validated Pipeline instance
    
    Example:
        >>> from reflowfy import build_pipeline, elastic_source, kafka_destination
        >>> from reflowfy.transformations import BaseTransformation
        >>> 
        >>> class MyTransform(BaseTransformation):
        ...     name = "my_transform"
        ...     def apply(self, records, context):
        ...         return [r.upper() for r in records]
        >>> 
        >>> pipeline = build_pipeline(
        ...     name="example_pipeline",
        ...     source=elastic_source(...),
        ...     transformations=[MyTransform()],
        ...     destination=kafka_destination(...),
        ...     rate_limit={"jobs_per_second": 100}
        ... )
    """
    pipeline = Pipeline(
        name=name,
        source=source,
        transformations=transformations,
        destination=destination,
        rate_limit=rate_limit,
        config=config or {},
    )
    
    return pipeline
