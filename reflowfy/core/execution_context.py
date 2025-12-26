"""Execution context for passing runtime state through the pipeline."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
import uuid
from jinja2 import Environment, BaseLoader, TemplateSyntaxError, UndefinedError


@dataclass
class ExecutionContext:
    """
    Runtime execution context passed through pipeline execution.
    
    Contains:
    - Execution tracking IDs
    - Runtime parameters provided by user
    - Metadata (timestamps, batch info, etc.)
    
    Used by:
    - API to track executions
    - Workers to execute jobs
    - Sources to resolve runtime parameters
    """
    
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    batch_id: Optional[str] = None
    pipeline_name: str = ""
    runtime_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        """Initialize batch_id if not provided."""
        if self.batch_id is None:
            self.batch_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize context for job metadata."""
        return {
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "pipeline_name": self.pipeline_name,
            "runtime_params": self.runtime_params,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
        }


class ParameterResolver:
    """
    Resolves Jinja2 template parameters in source configurations.
    
    Example:
        >>> resolver = ParameterResolver()
        >>> query = {"range": {"timestamp": {"gte": "{{ start_time }}"}}}
        >>> params = {"start_time": "2024-01-01"}
        >>> resolved = resolver.resolve(query, params)
        >>> # Result: {"range": {"timestamp": {"gte": "2024-01-01"}}}
    """
    
    def __init__(self):
        self.env = Environment(loader=BaseLoader())
    
    def resolve(self, obj: Any, params: Dict[str, Any]) -> Any:
        """
        Recursively resolve Jinja2 templates in nested objects.
        
        Args:
            obj: Object to resolve (dict, list, str, or primitive)
            params: Runtime parameters to inject
        
        Returns:
            Resolved object with template values replaced
        
        Raises:
            ValueError: If template syntax is invalid or parameter is missing
        """
        if isinstance(obj, dict):
            return {k: self.resolve(v, params) for k, v in obj.items()}
        
        elif isinstance(obj, list):
            return [self.resolve(item, params) for item in obj]
        
        elif isinstance(obj, str):
            # Check if string contains Jinja2 template syntax
            if "{{" in obj or "{%" in obj:
                try:
                    template = self.env.from_string(obj)
                    return template.render(**params)
                except TemplateSyntaxError as e:
                    raise ValueError(f"Invalid template syntax: {e}")
                except UndefinedError as e:
                    raise ValueError(f"Missing required parameter: {e}")
        
        # Return primitives as-is
        return obj
    
    def extract_parameters(self, obj: Any) -> set[str]:
        """
        Extract all parameter names from Jinja2 templates.
        
        Args:
            obj: Object to scan for parameters
        
        Returns:
            Set of parameter names used in templates
        """
        params = set()
        
        if isinstance(obj, dict):
            for v in obj.values():
                params.update(self.extract_parameters(v))
        
        elif isinstance(obj, list):
            for item in obj:
                params.update(self.extract_parameters(item))
        
        elif isinstance(obj, str):
            if "{{" in obj:
                # Simple regex-based extraction
                import re
                matches = re.findall(r'\{\{\s*(\w+)\s*\}\}', obj)
                params.update(matches)
        
        return params


# Global resolver instance
parameter_resolver = ParameterResolver()
