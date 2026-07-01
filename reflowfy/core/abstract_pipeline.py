"""Abstract base class for configurable pipelines.

This module provides the AbstractPipeline base class that allows users to create
pipelines with dynamic source/destination selection and conditional transformations
based on runtime parameters.

Example:
    >>> class MyPipeline(AbstractPipeline):
    ...     name = "my_pipeline"
    ...     rate_limit = 20
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
    ...     def define_destination(self, records, params):
    ...         return console_destination()
    ...
    ...     def define_transformations(self, records, params):
    ...         return [MyTransformation()]
"""

import re
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


class PipelineMeta(ABCMeta):
    """
    Metaclass for automatic pipeline registration.

    When a class inherits from AbstractPipeline and defines a 'name' attribute,
    it is automatically instantiated and registered in the pipeline registry.

    Inherits from ABCMeta to be compatible with ABC.
    """

    def __new__(mcs, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)

        # Only register concrete pipelines (not the base class)
        if name != "AbstractPipeline" and bases:
            # Validate cron expression at class-definition time (before instantiation)
            schedule = namespace.get("schedule")
            if schedule is not None:
                try:
                    from croniter import croniter as _croniter

                    if not _croniter.is_valid(schedule):
                        raise ValueError(
                            f"Pipeline '{namespace.get('name', name)}' has invalid cron "
                            f"expression: '{schedule}'. "
                            f"(reflowfy uses 5-field cron: minute hour day month weekday)"
                        )
                except ImportError:
                    pass  # croniter not installed; validated at runtime by scheduler

            # Check if this is a concrete pipeline with a name
            if "name" in namespace and namespace["name"]:
                # Import here to avoid circular dependency
                from reflowfy.core.registry import pipeline_registry

                try:
                    instance = cls()
                    pipeline_registry.register(instance)
                except Exception:
                    # Skip auto-registration if instantiation fails
                    # (e.g., missing required config)
                    pass

        return cls


@dataclass
class PipelineParameter:
    """
    Describes a parameter that the pipeline accepts at runtime.

    Attributes:
        name: Parameter name (used in API requests)
        description: Human-readable description
        required: Whether this parameter is mandatory
        param_type: Expected Python type (str, int, bool, float, list, dict)
        default: Default value if not provided
        choices: Optional list of valid values

    Example:
        >>> PipelineParameter(name="count", param_type=int, required=True)
        >>> PipelineParameter(name="env", param_type=str, choices=["dev", "prod"])
        >>> PipelineParameter(name="enabled", param_type=bool, default=False)
    """

    name: str
    description: str = ""
    required: bool = False
    param_type: type = str  # Actual Python type: str, int, bool, float, list, dict
    default: Any = None
    choices: Optional[List[Any]] = None

    # Type name mapping for serialization
    _TYPE_NAMES = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
        list: "array",
        dict: "object",
    }

    def validate(self, value: Any) -> Optional[str]:
        """
        Validate a value against this parameter's type and constraints.

        Args:
            value: The value to validate

        Returns:
            Error message string if invalid, None if valid
        """
        if value is None:
            if self.required:
                return f"Missing required parameter: {self.name}"
            return None

        # Type validation
        if not self._check_type(value):
            type_name = self._TYPE_NAMES.get(self.param_type, self.param_type.__name__)
            return f"Parameter '{self.name}' must be {type_name}, got {type(value).__name__}"

        # Choices validation
        if self.choices and value not in self.choices:
            return f"Invalid value for {self.name}: '{value}'. Must be one of: {self.choices}"

        return None

    def _check_type(self, value: Any) -> bool:
        """Check if value matches the expected type."""
        # Special handling for bool (since bool is subclass of int in Python)
        if self.param_type is bool:
            return isinstance(value, bool)

        # Special handling for int (don't accept bool as int)
        if self.param_type is int:
            return isinstance(value, int) and not isinstance(value, bool)

        # Special handling for float (accept int as float)
        if self.param_type is float:
            return isinstance(value, (int, float)) and not isinstance(value, bool)

        # Standard type check
        return isinstance(value, self.param_type)

    def coerce(self, value: Any) -> Any:
        """
        Attempt to coerce a value to the expected type.

        Useful for converting string inputs from APIs to proper types.

        Args:
            value: The value to coerce

        Returns:
            Coerced value, or original if coercion fails
        """
        if value is None:
            return self.default

        # Already correct type
        if self._check_type(value):
            return value

        try:
            if self.param_type is bool:
                # Handle string booleans
                if isinstance(value, str):
                    if value.lower() in ("true", "1", "yes", "on"):
                        return True
                    if value.lower() in ("false", "0", "no", "off"):
                        return False
                return bool(value)

            if self.param_type is int:
                return int(value)

            if self.param_type is float:
                return float(value)

            if self.param_type is str:
                return str(value)

            if self.param_type is list and isinstance(value, str):
                import json

                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    # Fallback: treat as comma-separated values
                    return [v.strip() for v in value.split(",") if v.strip()]

            if self.param_type is dict and isinstance(value, str):
                import json

                return json.loads(value)

        except (ValueError, TypeError):
            pass

        return value  # Return original if coercion fails

    def to_dict(self) -> Dict[str, Any]:
        """Serialize parameter info for API documentation."""
        type_name = self._TYPE_NAMES.get(self.param_type, self.param_type.__name__)
        result = {
            "name": self.name,
            "description": self.description,
            "required": self.required,
            "type": type_name,
        }
        if self.default is not None:
            result["default"] = self.default
        if self.choices:
            result["choices"] = self.choices
        return result


class AbstractPipeline(metaclass=PipelineMeta):
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
        rate_limit: Optional rate limiting config (e.g., 50)
        config: Additional pipeline-specific configuration
    """

    # Must be set by concrete subclass
    name: str = ""

    # Optional rate limit (can be overridden per-request via define_rate_limit)
    rate_limit: Optional[float] = None

    # Additional configuration
    config: Dict[str, Any] = {}

    # Duplicate job control:
    #   True  = jobs may run multiple times (default, current behavior)
    #   False = each unique job (by content hash) runs at most once
    enable_duplicate_jobs: bool = True

    # Optional cron schedule for automatic execution (e.g. "*/5 * * * *").
    # None means the pipeline is never auto-scheduled.
    schedule: Optional[str] = None

    def __init__(
        self,
        rate_limit: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
        enable_duplicate_jobs: Optional[bool] = None,
    ):
        """
        Initialize the abstract pipeline.

        Args:
            rate_limit: Rate limiting configuration
            config: Additional configuration options
            enable_duplicate_jobs: True = jobs may run multiple times (default);
                False = each unique job (by content hash) runs at most once
        """
        if rate_limit is not None:
            self.rate_limit = rate_limit
        if config is not None:
            self.config = config
        if enable_duplicate_jobs is not None:
            self.enable_duplicate_jobs = enable_duplicate_jobs

        # Validate pipeline name
        if not self.name:
            raise ValueError(f"{self.__class__.__name__} must define a 'name' attribute")

        if not re.match(r"^[a-zA-Z0-9_-]+$", self.name):
            raise ValueError(
                f"Pipeline name '{self.name}' must contain only alphanumeric "
                "characters, underscores, or hyphens"
            )

        # Validate cron expression if schedule is set
        if self.schedule is not None:
            try:
                from croniter import croniter as _croniter

                if not _croniter.is_valid(self.schedule):
                    raise ValueError(
                        f"Pipeline '{self.name}' has invalid cron expression: '{self.schedule}'"
                    )
            except ImportError:
                pass  # croniter not installed; validated at runtime by scheduler

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
    def define_destination(self, records: List[Any], runtime_params: Dict[str, Any]) -> Any:
        """
        Define the destination to use based on post-transformation records and runtime params.

        Args:
            records: Post-transformation records for the current job/batch
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseDestination instance

        Example:
            >>> def define_destination(self, records, params):
            ...     if len(records) > 1000:
            ...         return kafka_destination(topic="bulk-output")
            ...     return console_destination()
        """
        pass

    @abstractmethod
    def define_transformations(
        self, records: List[Any], runtime_params: Dict[str, Any]
    ) -> List[Any]:
        """
        Define list of transformations to apply based on records and runtime parameters.

        Args:
            records: Current records for this job/batch (before transformations)
            runtime_params: Parameters provided by the user at runtime

        Returns:
            List of BaseTransformation instances to apply in order

        Example:
            >>> def define_transformations(self, records, params):
            ...     transforms = [FilterActive()]
            ...     if len(records) > 1000:
            ...         transforms.append(ChunkLargePayloads())
            ...     if params.get("uppercase"):
            ...         transforms.append(UppercaseNames())
            ...     return transforms

        Note:
            This method is re-evaluated after each transformation is applied, so a
            transformation that adds a key to ``runtime_params`` can cause a later
            transformation to be appended to the returned list on the next pass.
            The list must be **append-only** with respect to growing
            ``runtime_params``: re-resolution may only grow the list. The ``records``
            argument is always the original pre-transformation records (it does not
            change between passes); only ``runtime_params`` changes. Already-applied
            transformations are never re-applied, and changes to earlier
            (already-applied) positions on a later pass are ignored.
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
            ...             param_type=bool,
            ...             default=False
            ...         ),
            ...     ]
        """
        return []

    def define_rate_limit(self, runtime_params: Dict[str, Any]) -> Optional[float]:
        """
        Define rate limit configuration based on runtime parameters.

        Override to provide dynamic rate limiting based on parameters.
        Default implementation returns the static rate_limit attribute.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            Jobs per second (float) or None

        Example:
            >>> def define_rate_limit(self, params):
            ...     if params.get("env") == "production":
            ...         return 10  # Slower for prod
            ...     return 100  # Faster for dev
        """
        return self.rate_limit

    def should_apply_transformation(
        self, transformation: Any, runtime_params: Dict[str, Any], records: List[Any]
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
            error = param.validate(value)
            if error:
                errors.append(error)

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
            return [t.name for t in self.define_transformations([], {})]
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
    _transformations: List[Any] | None = None

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
        # destination and transformations are resolved per-job/per-batch,
        # once records are available.
        self._destination = None
        self._transformations = None

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

    @property
    def is_scheduled(self) -> bool:
        """Return True if this pipeline has a cron schedule configured."""
        return self.schedule is not None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata for API responses."""
        return {
            "name": self.name,
            "parameters": [p.to_dict() for p in self.define_parameters()],
            "rate_limit": self.rate_limit,
            "config": self.config,
            "transformations": self.get_transformation_names(),
            "enable_duplicate_jobs": self.enable_duplicate_jobs,
            "schedule": self.schedule,
            "is_scheduled": self.is_scheduled,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"
