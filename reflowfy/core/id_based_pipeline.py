"""ID-based pipeline for dynamic per-ID execution.

This module provides the IdBasedPipeline base class that allows users to create
pipelines which iterate over a list of user-provided IDs, dynamically resolving
the source for each ID (or each batch of IDs when ids_batch_size > 1).

Example (single-ID mode, default):
    >>> class UserSyncPipeline(IdBasedPipeline):
    ...     name = "user_sync"
    ...     rate_limit = 20
    ...
    ...     def define_source(self, params):
    ...         # current_ids is available in runtime params
    ...         current_ids = params["current_ids"]
    ...         return paginated_api_source(
    ...             base_url="https://api.example.com",
    ...             endpoint=f"/users/{current_ids[0]}/records",
    ...         )
    ...
    ...     def define_destination(self, records, params):
    ...         return kafka_destination(topic="user-records")
    ...
    ...     def define_transformations(self, records, params):
    ...         return [EnrichWithId()]

Example (batch mode):
    >>> class BulkSyncPipeline(IdBasedPipeline):
    ...     name = "bulk_sync"
    ...     ids_batch_size = 10   # 10 IDs per source resolution
    ...
    ...     def define_source(self, params):
    ...         # current_ids is a list of up to 10 IDs in runtime params
    ...         return api_source(ids=params["current_ids"])
"""

import re
from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Set

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
        if name != "IdBasedPipeline" and bases:
            if "name" in namespace and namespace["name"]:
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
    calls `define_source(runtime_params)` for **each ID/batch**, with current
    IDs injected into runtime_params, allowing fully dynamic source configuration
    per entity.

    Subclasses MUST:
    - Set the `name` class attribute
    - Implement `define_source(runtime_params)`
    - Implement `define_destination(records, runtime_params)`
    - Implement `define_transformations(records, runtime_params)`

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
    # Set > 1 to process a list of IDs per source/transform/destination resolution.
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

        if not re.match(r"^[a-zA-Z0-9_-]+$", self.name):
            raise ValueError(
                f"Pipeline name '{self.name}' must contain only alphanumeric "
                "characters, underscores, or hyphens"
            )

    # =========================================================================
    # Abstract Methods — Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    def define_source(self, runtime_params: Dict[str, Any]) -> Any:
        """
        Define the source to use for a batch of IDs.

        Called once per ID-batch. Current IDs are injected into runtime_params:
        - runtime_params["current_ids"]: list of IDs for this batch
        - runtime_params["current_id"]: first ID in batch (convenience)

        May return either:
        - A configured ``BaseSource`` instance (fetched normally), or
        - A plain list of records. The list is used verbatim as the records for
          this batch (one job per batch) — no fetch happens. Use this to skip
          the source entirely when the IDs themselves are the data, e.g.
          ``return params["current_ids"]``.

        Args:
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseSource instance, or a list of records.

        Example (real source):
            >>> def define_source(self, params):
            ...     current_ids = params["current_ids"]
            ...     return paginated_api_source(
            ...         base_url="https://api.example.com",
            ...         endpoint=f"/entities/{current_ids[0]}/data",
            ...     )

        Example (skip the source — IDs are the records):
            >>> def define_source(self, params):
            ...     return params["current_ids"]
        """
        pass

    @abstractmethod
    def define_destination(self, records: List[Any], runtime_params: Dict[str, Any]) -> Any:
        """
        Define the destination using post-transformation records and runtime params.

        Args:
            records: Post-transformation records for this ID batch/job
            runtime_params: Parameters provided by the user at runtime

        Returns:
            A configured BaseDestination instance

        Example:
            >>> def define_destination(self, records, params):
            ...     return kafka_destination(topic="output")
        """
        pass

    @abstractmethod
    def define_transformations(
        self, records: List[Any], runtime_params: Dict[str, Any]
    ) -> List[Any]:
        """
        Define list of transformations to apply for a batch of IDs.

        Args:
            records: Current records for this batch (before transformations)
            runtime_params: Parameters provided by the user at runtime

        Returns:
            List of BaseTransformation instances to apply in order

        Example:
            >>> def define_transformations(self, records, params):
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

    def define_rate_limit(self, runtime_params: Dict[str, Any]) -> Optional[float]:
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
            return [
                t.name
                for t in self.define_transformations(
                    [], {"current_ids": ["__discovery__"], "current_id": "__discovery__"}
                )
            ]
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

    def resolve_source(self, batch_params: Dict[str, Any]) -> Any:
        """
        Call ``define_source`` and coerce its return value into a source.

        ``define_source`` may return either:
        - a ``BaseSource`` instance (fetched normally), or
        - a plain list of records, which is wrapped in a ``StaticSource`` so the
          records are used verbatim and no fetch happens. This lets ID-based
          pipelines feed runtime-provided IDs straight into the
          transformation/destination chain.

        Args:
            batch_params: Per-batch runtime params (with ``current_ids`` injected)

        Returns:
            A ``BaseSource`` instance.

        Raises:
            TypeError: If ``define_source`` returns neither a ``BaseSource`` nor a list.
        """
        from reflowfy.sources.base import BaseSource
        from reflowfy.sources.static import StaticSource

        returned = self.define_source(batch_params)
        if isinstance(returned, BaseSource):
            return returned
        if isinstance(returned, list):
            return StaticSource(returned)
        raise TypeError(
            f"Pipeline '{self.name}': define_source must return a BaseSource or a "
            f"list of records, got {type(returned).__name__}"
        )

    def resolve_for_ids(self, runtime_params: Dict[str, Any], ids_batch: List[Any]) -> dict:
        """
        Resolve source for a batch of IDs and prepare per-batch params.

        Called by the PipelineRunner once per ID-batch (batch size is
        determined by the pipeline's ``ids_batch_size`` attribute).

        Uses a per-batch copy of runtime_params so that any keys added by
        define_source (source enrichments) don't bleed into subsequent batches.
        The enrichments are returned separately so callers can inject them into
        the job payload metadata, making them available to workers.

        Args:
            runtime_params: Validated runtime parameters
            ids_batch: List of IDs in this batch

        Returns:
            Dict with 'source', 'transformations', 'destination', 'current_ids',
            'source_enrichments' (keys added by define_source), 'batch_params'
            (the full per-batch param dict including enrichments and current IDs).
        """
        # Use a fresh copy per batch — prevents enrichments from one batch
        # leaking into the next batch's runtime_params.
        batch_params = dict(runtime_params)
        batch_params["current_ids"] = list(ids_batch)
        batch_params["current_id"] = ids_batch[0] if ids_batch else None
        source = self.resolve_source(batch_params)
        source_enrichments = {k: batch_params[k] for k in batch_params if k not in runtime_params}
        return {
            "source": source,
            "transformations": None,
            "destination": None,
            "current_ids": ids_batch,
            "source_enrichments": source_enrichments,
            "batch_params": batch_params,
        }

    def resolve_for_id(self, runtime_params: Dict[str, Any], current_id: Any) -> dict:
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
