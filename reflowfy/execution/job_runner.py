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

import asyncio
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.core.exceptions import RecordValidationError, SkipJob, pipeline_step
from reflowfy.core.record_validation import describe_invalid, find_invalid_records
from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.transformation_runner import (
    AppliedStep,
    apply_transformations_iteratively_async,
)
from reflowfy.observability.logging import get_logger

logger = get_logger("execution.job_runner")

RECORD_TYPE_STEP = "record_type"
"""Name of the :class:`AppliedStep` that reports the ``record_type`` check."""


def check_record_type(pipeline: Any, records: List[Any]) -> Tuple[List[Any], AppliedStep]:
    """Check fetched ``records`` against ``pipeline.record_type``.

    Returns the records to transform and an :class:`AppliedStep` (mode
    ``"validate"``) for the check, so its duration and any skipped records are
    reported exactly like a transformation's. Invalid records fail the job
    (:class:`RecordValidationError`) unless ``on_invalid_record = "skip"``, which
    drops them with reasons such as ``invalid data.amount: missing``.
    """
    from reflowfy.transformations.base import count_skip_reason

    record_type = pipeline.record_type
    start = time.time()
    invalid = find_invalid_records(record_type, records)
    kept = records
    skip_reasons: Optional[Dict[str, int]] = None
    if invalid:
        message = describe_invalid(invalid, len(records), record_type)
        if getattr(pipeline, "on_invalid_record", "fail") != "skip":
            raise RecordValidationError(message, invalid, len(records))
        skip_reasons = {}
        for reason in invalid.values():
            count_skip_reason(skip_reasons, f"invalid {reason}")
        kept = [record for i, record in enumerate(records) if i not in invalid]
    step = AppliedStep(
        name=RECORD_TYPE_STEP,
        duration=time.time() - start,
        records_in=len(records),
        records_out=len(kept),
        mode="validate",
        skip_reasons=skip_reasons,
    )
    return kept, step


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
        ...     for entity_id in params.get("input_ids", []):
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
) -> Tuple[List[Any], List[Any], List[AppliedStep], Any]:
    """Synchronous form of :func:`run_job_records_async`, for the local/preview callers.

    One event loop per call, so every async step of one job shares it.
    """
    return run_coroutine_sync(run_job_records_async(source, pipeline, runtime_params, limit))


async def run_job_records_async(
    source: Any,
    pipeline: Any,
    runtime_params: Dict[str, Any],
    limit: Optional[int] = None,
) -> Tuple[List[Any], List[Any], List[AppliedStep], Any]:
    """Run the v2 per-job core for one (already-narrowed) source.

    Fetches the source's records, normalizes them to JSON-safe form (so
    ``datetime`` and other non-JSON-native values match what a destination's
    ``json.dumps`` receives), applies the pipeline's transformations
    dynamically, and resolves the destination against the transformed
    records.

    Synchronous user code (the fetch, sync transformations, ``define_destination``)
    runs in a thread and async transformations are awaited, so the caller's event
    loop stays free: on the worker, a blocked loop starves the Kafka heartbeat and
    the broker evicts the worker mid-job.

    The worker never passes ``limit`` (it processes the whole slice the
    manager assigned). The ``reflowfy test`` / local-preview callers pass a
    ``limit`` to cap the sample, truncating fetched records before
    transformation so the preview stays cheap and faithful.

    Does NOT send — the caller owns sending, stats, and presentation.

    Returns ``(records, transformed_records, applied, destination)`` where
    ``applied`` is the list of :class:`AppliedStep` from the
    transformation runner.
    """
    name = getattr(pipeline, "name", "<unknown>")
    try:
        with pipeline_step("source fetch", name):
            records = await asyncio.to_thread(lambda: to_json_safe(source.fetch(runtime_params)))
        if limit is not None:
            records = records[:limit]
        if not records:
            # Empty slice (e.g. Elastic sliced-scroll hash-partitions unevenly, so a
            # slice can match zero docs). Nothing to transform; skip so transformations
            # aren't invoked on []. Every caller already treats empty records as a no-op.
            return records, [], [], None
        checked = records
        checks: List[AppliedStep] = []
        if getattr(pipeline, "record_type", None) is not None:
            checked, check = await asyncio.to_thread(check_record_type, pipeline, records)
            checks.append(check)
        transformed_records, applied = await apply_transformations_iteratively_async(
            pipeline, checked, runtime_params
        )
        applied = checks + applied
        with pipeline_step("define_destination", name):
            destination = await asyncio.to_thread(
                pipeline.define_destination, transformed_records, runtime_params
            )
    except SkipJob as exc:
        # The pipeline dropped this job. Same shape as an empty slice, which every
        # caller already treats as a no-op — no destination write, no failure.
        # Reason rides on runtime_params (a per-job dict) so the caller can put it
        # in the job's stats/metrics without run_job_records growing a 5th return.
        runtime_params["skip_reason"] = str(exc)
        logger.debug(
            "Pipeline '%s': job dropped (%s)",
            name,
            exc,
            extra={"event": "skipjob_raised", "pipeline_name": name, "skip_reason": str(exc)},
        )
        return [], [], [], None
    return records, transformed_records, applied, destination
