"""ID-based pipeline for dynamic per-ID execution.

This module provides the IdBasedPipeline base class that allows users to create
pipelines which iterate over a list of user-provided IDs, dynamically resolving
the source for each ID (or each batch of IDs when ids_batch_size > 1).

Example (single-ID mode, default):
    >>> class UserSyncPipeline(IdBasedPipeline):
    ...     name = "user_sync"
    ...     rate_limit = 20
    ...
    ...     def define_source(self, params, current_ids):
    ...         # current_ids is always a list; [single_id] when ids_batch_size=1
    ...         return paginated_api_source(
    ...             base_url="https://api.example.com",
    ...             endpoint=f"/users/{current_ids[0]}/records",
    ...         )
    ...
    ...     def define_destination(self, params):
    ...         return kafka_destination(topic="user-records")
    ...
    ...     def define_transformations(self, params, current_ids):
    ...         return [EnrichWithId()]

Example (batch mode):
    >>> class BulkSyncPipeline(IdBasedPipeline):
    ...     name = "bulk_sync"
    ...     ids_batch_size = 10   # 10 IDs per source resolution
    ...
    ...     def define_source(self, params, current_ids):
    ...         # current_ids is a list of up to 10 IDs
    ...         return api_source(ids=current_ids)
"""

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Set
import re

from reflowfy.core.abstract_pipeline import PipelineParameter


class IdBasedPipelineMeta(ABCMeta):
    """
    Metaclass for automatic ID-based pipeline registration.

    When a class inherits from IdBasedPipeline and defines a 'name' attribute,
    it is automatically instantiated and registered in the pipeline registry.
    """

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)

        # Only register concrete pipelines (not the base class)
        if name != 'IdBasedPipeline' and bases:
            if 'name' in namespace and namespace['name']:
                from reflowfy.core.registry import pipeline_registry
                try:
                    instance = cls()
                    pipeline_registry.register(instance)
                except Exception:
                    pass

        return cls


# Built-in 'ids' parameter — automatically injected
_IDS_PARAMETER = PipelineParameter(
    name="ids",
    description="List of IDs to process. Each ID triggers a separate source resolution.",
    required=True,
    param_type=list,
)


class IdBasedPipeline(metaclass=IdBasedPipelineMeta):
    """
    Pipeline that executes dynamically for each ID in a user-provided list.

    Unlike AbstractPipeline (which has a single source), IdBasedPipeline
    calls `define_source(params, current_id)` for **each ID**, allowing
    fully dynamic source configuration per entity.

    Subclasses MUST:
    - Set the `name` class attribute
    - Implement `define_source(runtime_params, current_id)`
    - Implement `define_destination(runtime_params)`
    - Implement `define_transformations(runtime_params, current_id)`

    Subclasses MAY:
    - Override `define_parameters()` to add extra parameters (beyond `ids`)
    - Override `define_rate_limit()` for dynamic rate limiting

    The 'ids' parameter is automatically injected — users do NOT need to
    define it in `define_parameters()`.

    Attributes:
        name: Unique pipeline identifier (must be set by subclass)
        rate_limit: Optional rate limiting config (e.g., 50)
        config: Additional pipeline-specific configuration
    """

    # Must be set by concrete subclass
    name: str = ""

    # Optional rate limit
    rate_limit: Optional[float] = None

    # Number of IDs to group per source resolution.
    # Default 1 = one source call per ID (original behaviour).
    # Set > 1 to pass a list of IDs to define_source / define_transformations.
    ids_batch_size: int = 1

    # Additional configuration
    config: Dict[str, Any] = {}

    def __init__(
        self,
        rate_limit: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the ID-based pipeline.

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

    # =========================================================================
    # Abstract Methods — Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    def define_source(self, runtime_params: Dict[str, Any], current_ids: List[Any]) -> Any:
        """
        Define the source to use for a batch of IDs.

        Called once per ID-batch. When ids_batch_size=1 (default) the list
        contains a single element; when ids_batch_size=N it contains up to N IDs.

        Args:
            runtime_params: Parameters provided by the user at runtime
            current_ids: List of IDs in the current batch

        Returns:
            A configured BaseSource instance

        Example:
            >>> def define_source(self, params, current_ids):
            ...     return paginated_api_source(
            ...         base_url="https://api.example.com",
            ...         endpoint=f"/entities/{current_ids[0]}/data",
            ...     )
        """
        pass

    @abstractmethod
    def define_destination(self, runtime_params: Dict[str, Any]) -> Any:
        """
        Define the destination (shared across all IDs).

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseDestination instance

        Example:
            >>> def define_destination(self, params):
            ...     return kafka_destination(topic="output")
        """
        pass

    @abstractmethod
    def define_transformations(
        self, runtime_params: Dict[str, Any], current_ids: List[Any]
    ) -> List[Any]:
        """
        Define list of transformations to apply for a batch of IDs.

        Args:
            runtime_params: Parameters provided by the user at runtime
            current_ids: List of IDs in the current batch

        Returns:
            List of BaseTransformation instances to apply in order

        Example:
            >>> def define_transformations(self, params, current_ids):
            ...     return [
            ...         AddIdMetadata(),
            ...         FilterActive(),
            ...     ]
        """
        pass

    # =========================================================================
    # Optional Overrides
    # =========================================================================

    def define_parameters(self) -> List[PipelineParameter]:
        """
        Define additional parameters this pipeline accepts (beyond 'ids').

        The 'ids' parameter is automatically injected.
        Do NOT include 'ids' here — it will be added automatically.

        Returns:
            List of PipelineParameter instances

        Example:
            >>> def define_parameters(self):
            ...     return [
            ...         PipelineParameter(
            ...             name="env",
            ...             required=True,
            ...             choices=["dev", "prod"],
            ...         ),
            ...     ]
        """
        return []

    def define_rate_limit(
        self, runtime_params: Dict[str, Any]
    ) -> Optional[float]:
        """
        Define rate limit configuration based on runtime parameters.

        Default implementation returns the static rate_limit attribute.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            Jobs per second (float) or None
        """
        return self.rate_limit

    # =========================================================================
    # Built-in Logic — Parameters with auto-injected 'ids'
    # =========================================================================

    def get_all_parameters(self) -> List[PipelineParameter]:
        """
        Get all parameters including the built-in 'ids' parameter.

        Returns:
            List of all PipelineParameter instances (ids + user-defined)
        """
        user_params = self.define_parameters()

        # Ensure user didn't accidentally define 'ids'
        user_param_names = {p.name for p in user_params}
        if "ids" in user_param_names:
            raise ValueError(
                f"Pipeline '{self.name}': Do not define 'ids' in define_parameters(). "
                "It is automatically injected by IdBasedPipeline."
            )

        return [_IDS_PARAMETER] + user_params

    def get_required_parameters(self) -> Set[str]:
        """Get names of required parameters (always includes 'ids')."""
        return {p.name for p in self.get_all_parameters() if p.required}

    def validate_parameters(self, runtime_params: Dict[str, Any]) -> List[str]:
        """
        Validate runtime parameters against defined parameters.

        Args:
            runtime_params: Parameters to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        for param in self.get_all_parameters():
            value = runtime_params.get(param.name)
            error = param.validate(value)
            if error:
                errors.append(error)

        # Additional validation for ids
        ids = runtime_params.get("ids")
        if ids is not None:
            if not isinstance(ids, list):
                errors.append("Parameter 'ids' must be a list")
            elif len(ids) == 0:
                errors.append("Parameter 'ids' must not be empty")

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

        for param in self.get_all_parameters():
            if param.name not in result and param.default is not None:
                result[param.name] = param.default

        return result

    # =========================================================================
    # Utility Methods — Compatibility with execution engine
    # =========================================================================

    def get_transformation_names(self) -> List[str]:
        """
        Return list of all possible transformation names.

        Uses a dummy single-element batch to get the transformation list.
        """
        try:
            return [t.name for t in self.define_transformations({}, ["__discovery__"])]
        except Exception:
            return []

    def get_runtime_parameters(self) -> List[str]:
        """
        Return list of runtime parameter names (includes 'ids').

        Used by sources that have Jinja templates.
        """
        return [p.name for p in self.get_all_parameters()]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata for API responses."""
        return {
            "name": self.name,
            "type": "id_based",
            "parameters": [p.to_dict() for p in self.get_all_parameters()],
            "rate_limit": self.rate_limit,
            "ids_batch_size": self.ids_batch_size,
            "config": self.config,
            "transformations": self.get_transformation_names(),
        }

    # =========================================================================
    # Resolution — Per-ID source/transformation resolution
    # =========================================================================

    def resolve_for_ids(
        self, runtime_params: Dict[str, Any], ids_batch: List[Any]
    ) -> dict:
        """
        Resolve source and transformations for a batch of IDs.

        Called by the PipelineRunner once per ID-batch (batch size is
        determined by the pipeline's ``ids_batch_size`` attribute).

        Args:
            runtime_params: Validated runtime parameters
            ids_batch: List of IDs in this batch

        Returns:
            Dict with 'source', 'transformations', 'destination', 'current_ids'
        """
        return {
            "source": self.define_source(runtime_params, ids_batch),
            "transformations": self.define_transformations(runtime_params, ids_batch),
            "destination": self.define_destination(runtime_params),
            "current_ids": ids_batch,
        }

    def resolve_for_id(
        self, runtime_params: Dict[str, Any], current_id: Any
    ) -> dict:
        """Backward-compat shim: wraps single ID in a list and calls resolve_for_ids."""
        return self.resolve_for_ids(runtime_params, [current_id])

    def resolve(self, runtime_params: Dict[str, Any]) -> "IdBasedPipeline":
        """
        Validate and prepare runtime parameters (without resolving per-ID).

        This is called once before execution to validate parameters.
        Per-ID resolution happens in the runner via resolve_for_id().

        Args:
            runtime_params: Runtime parameters for this execution

        Returns:
            self (for chaining)
        """
        params = self.apply_defaults(runtime_params)
        errors = self.validate_parameters(params)
        if errors:
            raise ValueError(f"Invalid parameters: {'; '.join(errors)}")

        self._resolved_params = params
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', type='id_based')"
