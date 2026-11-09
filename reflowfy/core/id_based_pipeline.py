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
transformations, its own destination write, and its own retry. Wrap it in
``job(...)`` to tag the job with the params it should run with, the way the
built-in plan passes ``current_id``/``current_ids``:
    >>> from reflowfy import RuntimeParams, job
    >>>
    >>> class ChildSyncPipeline(IdBasedPipeline[RuntimeParams]):
    ...     name = "child_sync"
    ...
    ...     def define_jobs(self, runtime_params: RuntimeParams):
    ...         for parent_id in runtime_params.get("input_ids", []):
    ...             for child_id in fetch_child_ids(parent_id):
    ...                 yield job(
    ...                     [{"parent_id": parent_id, "child_id": child_id}],
    ...                     current_id=parent_id,
    ...                 )

``input_ids`` is the caller's whole list, and ``define_jobs`` is the only hook
that reads it: it runs once, before any job exists, so there is no "current"
slice yet. Everywhere downstream — ``define_source``, transformations, the
destination — read ``current_ids``/``current_id`` instead; the manager strips
``input_ids`` out of each job before dispatch, so reading it there would mean
every job processing every ID.

Note that overriding ``define_jobs`` replaces the built-in plan entirely, so
``current_ids`` is no longer filled in for you — pass what each job needs via
``job(...)``. If all you want is one job per ID, do not override it: that is
already what ``ids_batch_size = 1`` (the default) does.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, cast

from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.core.runtime_params import P

if TYPE_CHECKING:
    from reflowfy.sources.base import BaseSource
from reflowfy.core.planning import PlannedJob, chunk

logger = logging.getLogger(__name__)


# Built-in 'input_ids' parameter — automatically injected
_IDS_PARAMETER = PipelineParameter(
    name="input_ids",
    description="List of IDs to process. Each ID triggers a separate source resolution.",
    required=True,
    param_type=list,
)


class IdBasedPipeline(AbstractPipeline[P]):
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
    - Override `define_parameters()` to add extra parameters (beyond `input_ids`)
    - Override `define_rate_limit()` for dynamic rate limiting
    - Override `define_jobs()` to own the splitting entirely — e.g. to expand
      each input ID into several jobs (see the module docstring). Then
      `define_source` is never called and does not need to be implemented.
    - Call `load_query("name.sql")` to read a template from the project's
      `queries/` folder (see QueryLoaderMixin)

    The 'input_ids' parameter is automatically injected — users do NOT need to
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
    execution_only_params = ("input_ids",)

    # =========================================================================
    # Abstract Methods — Must be implemented by subclasses
    # =========================================================================

    def define_source(self, runtime_params: P) -> "BaseSource | List[Any]":
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

    def define_jobs(self, runtime_params: P) -> Iterator[Any]:
        """
        Plan one job per ``ids_batch_size`` IDs.

        Override this to own the splitting yourself — the ``input_ids``
        parameter is still validated and injected, so you keep passing IDs in
        the request body while deciding what a job is (see the module docstring
        for the one-ID-fans-out-to-many case).

        Each job carries its batch's params (see :meth:`plan_batch`).
        """
        # Cast the dict, not the result: pyright resolves `input_ids` off the
        # TypedDict and calls a cast on the value redundant, while mypy can't
        # see through the `P` TypeVar bound and needs one. Casting the mapping
        # satisfies both.
        params = cast(Dict[str, Any], runtime_params)
        for ids_batch in chunk(params.get("input_ids", []), self.ids_batch_size):
            yield self.plan_batch(params, ids_batch)

    # =========================================================================
    # Built-in Logic — Parameters with auto-injected 'input_ids'
    # =========================================================================

    def get_all_parameters(self) -> List[PipelineParameter]:
        """
        Get all parameters including the built-in 'input_ids' parameter.

        Returns:
            List of all PipelineParameter instances (input_ids + user-defined)
        """
        user_params = self.define_parameters()

        # Ensure user didn't accidentally define 'input_ids'
        user_param_names = {p.name for p in user_params}
        if "input_ids" in user_param_names:
            raise ValueError(
                f"Pipeline '{self.name}': Do not define 'input_ids' in define_parameters(). "
                "It is automatically injected by IdBasedPipeline."
            )

        return [_IDS_PARAMETER] + user_params

    def validate_parameters(self, runtime_params: P) -> List[str]:
        """Validate the shared parameter rules, plus the ones specific to `input_ids`."""
        errors = super().validate_parameters(runtime_params)

        ids: Any = runtime_params.get("input_ids")
        if ids is not None:
            if not isinstance(ids, list):
                errors.append("Parameter 'input_ids' must be a list")
            elif len(cast(List[Any], ids)) == 0:
                errors.append("Parameter 'input_ids' must not be empty")

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
                    [],
                    cast(P, {"current_ids": ["__discovery__"], "current_id": "__discovery__"}),
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
        Call ``define_source`` for one batch and check what it returned.

        ``define_source`` may return either a source, or a plain list of records,
        which becomes one job carrying them verbatim (no fetch), so an ID-based
        pipeline can feed the runtime-provided IDs straight into its
        transformations.

        Raises:
            TypeError: ``define_source`` returned neither a source nor a list.
        """
        # Any, not the declared return type: the checks below are for the
        # unannotated pipelines the type system can't speak for.
        returned: Any = self.define_source(cast(P, batch_params))
        if isinstance(returned, list) or callable(getattr(returned, "fetch", None)):
            return cast(Any, returned)
        raise TypeError(
            f"Pipeline '{self.name}': define_source must return a BaseSource or a "
            f"list of records, got {type(returned).__name__}"
        )

    def plan_batch(self, runtime_params: Dict[str, Any], ids_batch: List[Any]) -> PlannedJob:
        """The job for one batch of IDs.

        ``define_source`` gets a copy of the params with ``current_ids`` /
        ``current_id`` set, so keys it adds stay with this batch and do not leak
        into the next one; the job carries that copy (enrichments included), and
        ``input_ids`` is left to the execution (``execution_only_params``).
        """
        batch_params = dict(runtime_params)
        batch_params["current_ids"] = list(ids_batch)
        batch_params["current_id"] = ids_batch[0] if ids_batch else None
        source = self.resolve_source(batch_params)
        return PlannedJob(
            source, {k: v for k, v in batch_params.items() if k not in self.execution_only_params}
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', type='id_based')"
