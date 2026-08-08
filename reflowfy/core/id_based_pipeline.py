"""ID-based pipeline for dynamic per-ID execution.

This module provides the IdBasedPipeline base class that allows users to create
pipelines which iterate over a list of user-provided IDs, dynamically resolving
the source for each ID (or each batch of IDs when ids_batch_size > 1).

It is an :class:`~reflowfy.core.abstract_pipeline.AbstractPipeline` whose
:meth:`~reflowfy.core.abstract_pipeline.AbstractPipeline.define_jobs` is already
written for you: one job per ID (or per ``ids_batch_size`` IDs).

Example (single-ID mode, default):
    >>> class UserSyncPipeline(IdBasedPipeline):
    ...     name = "user_sync"
    ...     rate_limit = 20
    ...
    ...     def define_source(self, params):
    ...         # current_ids is available in runtime params
    ...         return id_based_api_source(
    ...             base_url="https://api.example.com",
    ...             endpoint_template="/users/{id}/records",
    ...             ids=params["current_ids"],
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
    ...     def define_source(self, runtime_params):
    ...         # current_ids is a list of up to 10 IDs in runtime params
    ...         return api_source(ids=runtime_params["current_ids"])

Example (one input ID fans out into many jobs) — override ``define_jobs`` to own
the splitting. Each yielded element is one job, so each child ID gets its own
transformations, its own destination write, and its own retry:
    >>> class ChildSyncPipeline(IdBasedPipeline):
    ...     name = "child_sync"
    ...
    ...     def define_jobs(self, runtime_params):
    ...         for parent_id in runtime_params["ids"]:
    ...             for child_id in fetch_child_ids(parent_id):
    ...                 yield [{"parent_id": parent_id, "child_id": child_id}]
"""

import logging
from typing import Any, Dict, Iterator, List

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.execution.job_runner import chunk

logger = logging.getLogger(__name__)


# Built-in 'ids' parameter — automatically injected
_IDS_PARAMETER = PipelineParameter(
    name="ids",
    description="List of IDs to process. Each ID triggers a separate source resolution.",
    required=True,
    param_type=list,
)


class IdBasedPipeline(AbstractPipeline):
    """
    Pipeline that executes dynamically for each ID in a user-provided list.

    Where a plain AbstractPipeline has one source that splits itself, an
    IdBasedPipeline calls `define_source(runtime_params)` for **each ID/batch**,
    with the current IDs injected into runtime_params, allowing fully dynamic
    source configuration per entity.

    Subclasses MUST:
    - Set the `name` class attribute
    - Implement `define_source(runtime_params)` OR `define_jobs(runtime_params)`
    - Implement `define_destination(records, runtime_params)`
    - Implement `define_transformations(records, runtime_params)`

    Subclasses MAY:
    - Override `define_parameters()` to add extra parameters (beyond `ids`)
    - Override `define_rate_limit()` for dynamic rate limiting
    - Override `define_jobs()` to own the splitting entirely — e.g. to expand
      each input ID into several jobs (see the module docstring). Then
      `define_source` is never called and does not need to be implemented.
    - Call `load_query("name.sql")` to read a template from the project's
      `queries/` folder (see QueryLoaderMixin)

    The 'ids' parameter is automatically injected — users do NOT need to
    define it in `define_parameters()`.

    Attributes:
        name: Unique pipeline identifier (must be set by subclass)
        rate_limit: Optional rate limit in jobs per minute (e.g., 50)
        ids_batch_size: Number of IDs grouped per source resolution
        config: Additional pipeline-specific configuration
    """

    # Number of IDs to group per source resolution.
    # Default 1 = one source call per ID (original behaviour).
    # Set > 1 to process a list of IDs per source/transform/destination resolution.
    ids_batch_size: int = 1

    # =========================================================================
    # Abstract Methods — Must be implemented by subclasses
    # =========================================================================

    def define_source(self, runtime_params: Dict[str, Any]) -> Any:
        """
        Define the source to use for a batch of IDs.

        Optional: implement this **or** :meth:`define_jobs`, not both. The
        default job plan calls this once per ID-batch, so overriding
        ``define_jobs`` to own the splitting means you never need it.

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
            ...     return id_based_api_source(
            ...         base_url="https://api.example.com",
            ...         endpoint_template="/entities/{id}/data",
            ...         ids=params["current_ids"],
            ...     )

        Example (skip the source — IDs are the records):
            >>> def define_source(self, params):
            ...     return params["current_ids"]
        """
        raise NotImplementedError(
            f"Pipeline '{self.name}' must implement define_source() or define_jobs()"
        )

    # =========================================================================
    # Job planning — one job per ID batch
    # =========================================================================

    def define_jobs(self, runtime_params: Dict[str, Any]) -> Iterator[Any]:
        """
        Plan one job per ``ids_batch_size`` IDs.

        Override this to own the splitting yourself — the ``ids`` parameter is
        still validated and injected, so you keep passing IDs in the request
        body while deciding what a job is (see the module docstring for the
        one-ID-fans-out-to-many case).

        Each planned source carries its own ``job_params``: the batch's params
        without the full ``ids`` list, since a job that handles two IDs has no
        use for the other 999,998. ``current_ids``, ``current_id`` and any keys
        ``define_source`` added stay, so workers still see them.
        """
        for ids_batch in chunk(runtime_params.get("ids", []), self.ids_batch_size):
            resolved = self.resolve_for_ids(runtime_params, ids_batch)
            source = resolved["source"]
            source.job_params = {k: v for k, v in resolved["batch_params"].items() if k != "ids"}
            yield source

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

    def validate_parameters(self, runtime_params: Dict[str, Any]) -> List[str]:
        """Validate the shared parameter rules, plus the ones specific to `ids`."""
        errors = super().validate_parameters(runtime_params)

        ids = runtime_params.get("ids")
        if ids is not None:
            if not isinstance(ids, list):
                errors.append("Parameter 'ids' must be a list")
            elif len(ids) == 0:
                errors.append("Parameter 'ids' must not be empty")

        return errors

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

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline metadata for API responses."""
        return {**super().to_dict(), "type": "id_based", "ids_batch_size": self.ids_batch_size}

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

    def resolve_for_ids(
        self, runtime_params: Dict[str, Any], ids_batch: List[Any]
    ) -> Dict[str, Any]:
        """
        Resolve source for a batch of IDs and prepare per-batch params.

        Called by :meth:`define_jobs` once per ID-batch (batch size is
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

    def resolve_for_id(self, runtime_params: Dict[str, Any], current_id: Any) -> Dict[str, Any]:
        """Backward-compat shim: wraps single ID in a list and calls resolve_for_ids."""
        return self.resolve_for_ids(runtime_params, [current_id])

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', type='id_based')"
