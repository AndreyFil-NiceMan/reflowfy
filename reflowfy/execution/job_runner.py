"""Shared v2 per-job execution core.

A single source of truth for how one job's records flow from a (narrowed)
source through transformations to a resolved destination. Used by:

- ``reflowfy/worker/executor.py`` (distributed/local-dispatcher worker),
- ``reflowfy/execution/local_executor.py`` (API local-mode executor),
- ``reflowfy/cli/commands/test.py`` (``reflowfy test``).

Each caller owns its own concerns around this core (slice planning via
:func:`plan_slices`, sending, stats, presentation), but the fetch →
normalize → transform → resolve-destination sequence lives here so the
test/preview paths cannot drift from what the worker actually runs.
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from reflowfy.core.exceptions import pipeline_step
from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.transformation_runner import apply_transformations_iteratively


def chunk(records: List[Any], size: int = 1) -> List[List[Any]]:
    """Split ``records`` into per-job chunks of at most ``size`` items.

    Intended for :meth:`AbstractPipeline.define_jobs`, whose return value is the
    job plan — one job per element. ``chunk(rows, size=1)`` gives one job per
    record; the last chunk holds the remainder.

    Args:
        records: The records to split.
        size: Maximum number of records per job. Must be >= 1.

    Returns:
        A list of record chunks, one per job.

    Example:
        >>> chunk([1, 2, 3], size=2)
        [[1, 2], [3]]
    """
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    return [records[i : i + size] for i in range(0, len(records), size)]


def job(records: List[Any], **params: Any) -> Any:
    """One job carrying ``records``, plus the params that job alone should see.

    ``define_jobs`` decides what a job is; this is how it says what a job is
    *about*. The params are merged into that job's ``runtime_params``, so
    transformations and ``define_destination`` can read them — the way the
    built-in ID-based plan passes ``current_id``/``current_ids``. Without it a
    custom plan's jobs all share the execution's params and cannot tell each
    other apart.

    Args:
        records: The records for this one job.
        **params: Extra runtime_params for this job only.

    Returns:
        A source carrying the records and the per-job params.

    Example:
        >>> def define_jobs(self, params):
        ...     for entity_id in params["ids"]:
        ...         yield job(fetch_rows(entity_id), current_id=entity_id)
    """
    from reflowfy.sources.static import StaticSource

    source = StaticSource(records)
    source.job_params = params
    return source


def job_runtime_params(source: Any, runtime_params: Dict[str, Any]) -> Dict[str, Any]:
    """The runtime_params one planned job runs with: the execution's, plus its own.

    Keeps the local/preview runners agreeing with what the manager writes into
    each dispatched job's metadata.
    """
    return {**runtime_params, **(getattr(source, "job_params", None) or {})}


def plan_slices(source: Any, runtime_params: Dict[str, Any]) -> Iterator[Any]:
    """Yield the per-job narrowed sources for ``source``.

    Uses :meth:`BaseSource.split` when the source provides it (the v2
    planning hook). Falls back to a single job — the source itself — for
    duck-typed or custom sources that don't implement ``split``, so local
    preview/test runs work with any source shape.

    Any other iterable is the job plan itself (what ``define_jobs`` returns):
    each element is one job, either a list of records — wrapped in a
    :class:`StaticSource` so it travels in the job payload — or a source that
    splits itself. A generator works and is consumed lazily, so a
    ``define_jobs`` that ``yield``\\ s plans an unbounded number of jobs in
    constant memory; the caller writes each job out as it arrives.
    """
    split = getattr(source, "split", None)
    if split is not None:
        # split() builds new narrowed sources, so anything the planner attached
        # to the parent (job_params — the params this job's records belong to)
        # has to be carried onto each child, or it is lost before dispatch.
        parent_job_params = getattr(source, "job_params", None)
        for narrowed in split(runtime_params):
            if parent_job_params is not None and getattr(narrowed, "job_params", None) is None:
                narrowed.job_params = parent_job_params
            yield narrowed
        return

    if isinstance(source, (str, bytes, dict)) or not hasattr(source, "__iter__"):
        # Duck-typed or custom source with no split() — one job, the source itself.
        yield source
        return

    from reflowfy.sources.base import BaseSource
    from reflowfy.sources.static import StaticSource

    for item in source:
        if isinstance(item, BaseSource):
            yield from plan_slices(item, runtime_params)
        elif isinstance(item, list):
            # Empty chunk = no job, consistent with StaticSource.split().
            if item:
                # ponytail: records ride in the job payload; chunks approaching
                # Kafka's 1MB limit need a custom BaseSource that re-fetches.
                yield StaticSource(cast(List[Any], item))
        else:
            raise TypeError(
                "define_jobs returned an iterable, so each element must be one job: "
                f"a list of records or a BaseSource, got {type(item).__name__}. "
                "Wrap records with chunk(records, size=N), or return [records] "
                "for a single job."
            )


def run_job_records(
    source: Any,
    pipeline: Any,
    runtime_params: Dict[str, Any],
    limit: Optional[int] = None,
) -> Tuple[List[Any], List[Any], List[Tuple[str, float]], Any]:
    """Run the v2 per-job core for one (already-narrowed) source.

    Fetches the source's records, normalizes them to JSON-safe form (so
    ``datetime`` and other non-JSON-native values match what a destination's
    ``json.dumps`` receives), applies the pipeline's transformations
    dynamically, and resolves the destination against the transformed
    records.

    The worker never passes ``limit`` (it processes the whole slice the
    manager assigned). The ``reflowfy test`` / local-preview callers pass a
    ``limit`` to cap the sample, truncating fetched records before
    transformation so the preview stays cheap and faithful.

    Does NOT send — the caller owns sending, stats, and presentation.

    Returns ``(records, transformed_records, applied, destination)`` where
    ``applied`` is the list of ``(name, duration)`` pairs from the
    transformation runner.
    """
    name = getattr(pipeline, "name", "<unknown>")
    with pipeline_step("source fetch", name):
        records = to_json_safe(source.fetch(runtime_params))
    if limit is not None:
        records = records[:limit]
    if not records:
        # Empty slice (e.g. Elastic sliced-scroll hash-partitions unevenly, so a
        # slice can match zero docs). Nothing to transform; skip so transformations
        # aren't invoked on []. Every caller already treats empty records as a no-op.
        return records, [], [], None
    transformed_records, applied = apply_transformations_iteratively(
        pipeline, records, runtime_params
    )
    with pipeline_step("define_destination", name):
        destination = pipeline.define_destination(transformed_records, runtime_params)
    return records, transformed_records, applied, destination
