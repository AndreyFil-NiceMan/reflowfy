"""Abstract base class for configurable pipelines.

This module provides the AbstractPipeline base class that allows users to create
pipelines with dynamic source/destination selection and conditional transformations
based on runtime parameters.

Example:
    >>> class MyPipeline(AbstractPipeline):
    ...     name = "my_pipeline"
    ...     rate_limit = {"jobs_per_second": 20}
    ...     
    ...     def define_parameters(self):
    ...         return [
    ...             PipelineParameter(name="env", required=True, choices=["dev", "prod"]),
    ...         ]
    ...     
    ...     def define_source(self, params):
    ...         if params.get("env") == "prod":
    ...             return elastic_source(...)
    ...         return mock_source(...)
    ...     
    ...     def define_destination(self, params):
    ...         return console_destination()
    ...     
    ...     def define_transformations(self, params):
    ...         return [MyTransformation()]
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
import re


@dataclass
class PipelineParameter:
    """
    Describes a parameter that the pipeline accepts at runtime.
    
    Attributes:
        name: Parameter name (used in API requests)
        description: Human-readable description
        required: Whether this parameter is mandatory
        param_type: Expected type (str, int, bool, etc.)
        default: Default value if not provided
        choices: Optional list of valid values
    """
    name: str
    description: str = ""
    required: bool = False
    param_type: str = "str"
    default: Any = None
    choices: Optional[List[Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize parameter info for API documentation."""
        result = {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "type": self.param_type,
        }
        if self.default is not None:
            result["default"] = self.default
        if self.choices:
            result["choices"] = self.choices
        return result


class AbstractPipeline(ABC):
    """
    Abstract base class for configurable pipelines.
    
    Extend this class to create pipelines with dynamic source/destination
    selection and conditional transformations based on runtime parameters.
    
    Subclasses MUST:
    - Set the `name` class attribute
    - Implement `define_source()`
    - Implement `define_destination()`
    - Implement `define_transformations()`
    
    Subclasses MAY:
    - Override `define_parameters()` to expose required runtime parameters
    - Override `define_rate_limit()` for dynamic rate limiting
    - Override `should_apply_transformation()` for runtime condition checks
    
    Attributes:
        name: Unique pipeline identifier (must be set by subclass)
        rate_limit: Optional rate limiting config (e.g., {"jobs_per_second": 50})
        config: Additional pipeline-specific configuration
    """
    
    # Must be set by concrete subclass
    name: str = ""
    
    # Optional rate limit (can be overridden per-request via define_rate_limit)
    rate_limit: Optional[Dict[str, int]] = None
    
    # Additional configuration
    config: Dict[str, Any] = {}
    
    def __init__(
        self,
        rate_limit: Optional[Dict[str, int]] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the abstract pipeline.
        
        Args:
            rate_limit: Rate limiting configuration
            config: Additional configuration options
        """
        if rate_limit is not None:
            self.rate_limit = rate_limit
        if config is not None:
            self.config = config
        
        # Validate pipeline name
        if not self.name:
            raise ValueError(f"{self.__class__.__name__} must define a 'name' attribute")
        
        if not re.match(r'^[a-zA-Z0-9_-]+$', self.name):
            raise ValueError(
                f"Pipeline name '{self.name}' must contain only alphanumeric "
                "characters, underscores, or hyphens"
            )
    
    @abstractmethod
    def define_source(self, runtime_params: Dict[str, Any]) -> Any:
        """
        Define the source to use based on runtime parameters.
        
        Args:
            runtime_params: Parameters provided by the user at runtime
        
        Returns:
            A configured BaseSource instance
        
        Example:
            >>> def define_source(self, params):
            ...     if params.get("env") == "production":
            ...         return elastic_source(url="http://prod:9200", ...)
            ...     return mock_source(data=[...])
        """
        pass
    
    @abstractmethod
    def define_destination(self, runtime_params: Dict[str, Any]) -> Any:
        """
        Define the destination to use based on runtime parameters.
        
        Args:
            runtime_params: Parameters provided by the user at runtime
        
        Returns:
            A configured BaseDestination instance
        
        Example:
            >>> def define_destination(self, params):
            ...     if params.get("output") == "kafka":
            ...         return kafka_destination(topic="output")
            ...     return console_destination()
        """
        pass
    
    @abstractmethod
    def define_transformations(self, runtime_params: Dict[str, Any]) -> List[Any]:
        """
        Define list of transformations to apply based on runtime parameters.
        
        Args:
            runtime_params: Parameters provided by the user at runtime
        
        Returns:
            List of BaseTransformation instances to apply in order
        
        Example:
            >>> def define_transformations(self, params):
            ...     transforms = [FilterActive()]
            ...     if params.get("uppercase"):
            ...         transforms.append(UppercaseNames())
            ...     return transforms
        """
        pass
    
    def define_parameters(self) -> List[PipelineParameter]:
        """
        Define the parameters this pipeline accepts.
        
        Override this method to expose the parameters users need to provide
        when running this pipeline. This is used for API documentation and
        validation.
        
        Returns:
            List of PipelineParameter instances
        
        Example:
            >>> def define_parameters(self):
            ...     return [
            ...         PipelineParameter(
            ...             name="env",
            ...             description="Environment to use",
            ...             required=True,
            ...             choices=["dev", "staging", "production"]
            ...         ),
            ...         PipelineParameter(
            ...             name="uppercase",
            ...             description="Apply uppercase transformation",
            ...             param_type="bool",
            ...             default=False
            ...         ),
            ...     ]
        """
        return []
    
    def define_rate_limit(self, runtime_params: Dict[str, Any]) -> Optional[Dict[str, int]]:
        """
        Define rate limit configuration based on runtime parameters.
        
        Override to provide dynamic rate limiting based on parameters.
        Default implementation returns the static rate_limit attribute.
        
        Args:
            runtime_params: Parameters provided by the user at runtime
        
        Returns:
            Rate limit config dict or None
        
        Example:
            >>> def define_rate_limit(self, params):
            ...     if params.get("env") == "production":
            ...         return {"jobs_per_second": 10}  # Slower for prod
            ...     return {"jobs_per_second": 100}  # Faster for dev
        """
        return self.rate_limit
    
    def should_apply_transformation(
        self,
        transformation: Any,
        runtime_params: Dict[str, Any],
        records: List[Any]
    ) -> bool:
        """
        Determine if a transformation should be applied at runtime.
        
        Override to conditionally skip transformations based on runtime state.
        Default implementation returns True (apply all transformations).
        
        Args:
            transformation: The transformation instance
            runtime_params: Runtime parameters
            records: Current batch of records
        
        Returns:
            True if transformation should be applied, False to skip
        """
        return True
    
    # =========================================================================
    # Utility Methods (not meant to be overridden)
    # =========================================================================
    
    def get_required_parameters(self) -> Set[str]:
        """Get names of required parameters."""
        return {p.name for p in self.define_parameters() if p.required}
    
    def validate_parameters(self, runtime_params: Dict[str, Any]) -> List[str]:
        """
        Validate runtime parameters against defined parameters.
        
        Args:
            runtime_params: Parameters to validate
        
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        for param in self.define_parameters():
            value = runtime_params.get(param.name)
            
            # Check required
            if param.required and value is None:
                errors.append(f"Missing required parameter: {param.name}")
                continue
            
            # Check choices if value provided
            if value is not None and param.choices and value not in param.choices:
                errors.append(
                    f"Invalid value for {param.name}: '{value}'. "
                    f"Must be one of: {param.choices}"
                )
        
        return errors
    
    def apply_defaults(self, runtime_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply default values to runtime parameters.
        
        Args:
            runtime_params: User-provided parameters
        
        Returns:
            Parameters with defaults applied
        """
        result = dict(runtime_params)
        
        for param in self.define_parameters():
            if param.name not in result and param.default is not None:
                result[param.name] = param.default
        
        return result
    
    def get_transformation_names(self) -> List[str]:
        """
        Return list of all possible transformation names.
        
        This is used for worker registration.
        """
        try:
            return [t.name for t in self.define_transformations({})]
        except Exception:
            return []
    
    def get_runtime_parameters(self) -> List[str]:
        """
        Return list of runtime parameter names.
        
        Used by sources that have Jinja templates (e.g., Elasticsearch queries).
        """
        return [p.name for p in self.define_parameters()]
    
    # =========================================================================
    # Execution Support - Properties for compatibility with execution engine
    # =========================================================================
    
    _resolved_params: Optional[Dict[str, Any]] = None
    _source: Any = None
    _destination: Any = None
    _transformations: List[Any] = None
    
    def resolve(self, runtime_params: Dict[str, Any]) -> "AbstractPipeline":
        """
        Resolve the pipeline with specific runtime parameters.
        
        This method prepares the pipeline for execution by calling all
        define_* methods and caching the results. Should be called before
        passing the pipeline to an executor.
        
        Args:
            runtime_params: Runtime parameters for this execution
        
        Returns:
            self (for chaining)
        """
        # Apply defaults and validate
        params = self.apply_defaults(runtime_params)
        errors = self.validate_parameters(params)
        if errors:
            raise ValueError(f"Invalid parameters: {'; '.join(errors)}")
        
        self._resolved_params = params
        self._source = self.define_source(params)
        self._destination = self.define_destination(params)
        self._transformations = self.define_transformations(params)
        
        return self
    
    @property
    def source(self) -> Any:
        """Get the resolved source. Call resolve() first."""
        if self._source is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._source
    
    @property
    def destination(self) -> Any:
        """Get the resolved destination. Call resolve() first."""
        if self._destination is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._destination
    
    @property
    def transformations(self) -> List[Any]:
        """Get the resolved transformations. Call resolve() first."""
        if self._transformations is None:
            raise RuntimeError(
                "Pipeline not resolved. Call pipeline.resolve(runtime_params) before execution."
            )
        return self._transformations
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata for API responses."""
        return {
            "name": self.name,
            "parameters": [p.to_dict() for p in self.define_parameters()],
            "rate_limit": self.rate_limit,
            "config": self.config,
            "transformations": self.get_transformation_names(),
        }
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
