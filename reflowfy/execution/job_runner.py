"""Shared v2 per-job execution core.

A single source of truth for how one job's records flow from a (narrowed)
source through transformations to a resolved destination. Used by:

- ``reflowfy/worker/executor.py`` (distributed/local-dispatcher worker),
- ``reflowfy/cli/commands/test.py`` (``reflowfy test``).

Each caller owns its own concerns around this core (slice planning via
:func:`plan_slices`, sending, stats, presentation), but the fetch →
normalize → transform → resolve-destination sequence lives here so the
test/preview paths cannot drift from what the worker actually runs.
"""

import asyncio
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple, cast

from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.core.execution_context import ExecutionContext
from reflowfy.core.exceptions import RecordValidationError, SkipJob, pipeline_step
from reflowfy.core.planning import PlannedJob
from reflowfy.core.planning import chunk as chunk  # its old home; kept importable
from reflowfy.core.record_validation import describe_invalid, find_invalid_records
from reflowfy.core.serialization import to_json_safe
from reflowfy.destinations.fan_out import as_destination
from reflowfy.destinations.record import RecordDestination
from reflowfy.execution.transformation_runner import (
    AppliedStep,
    StepMode,
    apply_transformations_iteratively_async,
)
from reflowfy.observability.logging import get_logger
from reflowfy.core.record_steps import count_skip_reason, top_skip_reasons

logger = get_logger("execution.job_runner")

RECORD_TYPE_STEP = "record_type"
"""Name of the :class:`AppliedStep` that reports the ``record_type`` check."""

ROUTE_MODE = StepMode.ROUTE
"""``AppliedStep.mode`` of a ``@record_destination`` choosing each record's destination.

Not a transformation: reported as a step so its time and skipped records show up
wherever a transformation's do."""


def _route_concurrency(destination: RecordDestination) -> Optional[int]:
    """Records routed at once, for an ``async def`` destination function; else None."""
    import inspect

    if not inspect.iscoroutinefunction(destination.destination_for_record):
        return None
    return destination.effective_concurrency()


def check_record_type(pipeline: Any, records: List[Any]) -> Tuple[List[Any], AppliedStep]:
    """Check fetched ``records`` against ``pipeline.record_type``.

    Returns the records to transform and an :class:`AppliedStep` (mode
    ``"validate"``) for the check, so its duration and any skipped records are
    reported exactly like a transformation's. Invalid records fail the job
    (:class:`RecordValidationError`) unless ``on_invalid_record = "skip"``, which
    drops them with reasons such as ``invalid data.amount: missing``.
    """

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
        mode=StepMode.VALIDATE,
        skip_reasons=skip_reasons,
    )
    return kept, step


def log_job_dropped(log: Any, reason: str) -> None:
    """The ``job_dropped`` line for a job a pipeline dropped with ``SkipJob``: one
    wording for the worker and ``reflowfy test`` (each passes its own logger)."""
    log.info(
        "Job dropped by pipeline (%s)",
        reason,
        extra={"event": "job_dropped", "skip_reason": reason},
    )


def log_skipped_records(log: Any, step: AppliedStep) -> None:
    """The ``records_skipped`` line for a step that dropped records, if it did.

    Reasons stay in the message: as a field their keys would be unbounded mapping
    names in the log index.
    """
    if not step.records_skipped:
        return
    log.info(
        "%s skipped %d records (%s)",
        step.name,
        step.records_skipped,
        top_skip_reasons(step.skip_reasons or {}),
        extra={
            "event": "records_skipped",
            "transformation": step.name,
            "records_skipped": step.records_skipped,
        },
    )


def job(records: List[Any], **params: Any) -> PlannedJob:
    """One job carrying ``records``, plus the params that job alone should see.

    ``define_jobs`` decides what a job is; this is how it says what a job is
    *about*. The params are merged into that job's ``runtime_params``, so
    transformations and ``define_destination`` can read them — the way the
    built-in ID-based plan passes ``current_id``/``current_ids``. Without it a
    custom plan's jobs all share the execution's params and cannot tell each
    other apart.

    Example:
        >>> def define_jobs(self, params):
        ...     for entity_id in params.get("input_ids", []):
        ...         yield job(fetch_rows(entity_id), current_id=entity_id)
    """
    return PlannedJob(records, params)


def preview_runtime_params(pipeline_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """The flat runtime_params a local run starts from: ``params`` plus the reserved keys.

    For the callers that run a pipeline outside the manager (``reflowfy test`` and
    ``reflowfy.testing``), which have no execution row to take the IDs from.
    """
    context = ExecutionContext(
        execution_id=str(uuid.uuid4()),
        pipeline_name=pipeline_name,
        runtime_params=params,
    )
    flat = dict(params)
    flat.update(
        {
            "execution_id": context.execution_id,
            "batch_id": context.batch_id,
            "pipeline_name": context.pipeline_name,
            "created_at": context.created_at.isoformat(),
        }
    )
    return flat


def plan_slices(jobs: Any, runtime_params: Dict[str, Any]) -> Iterator[PlannedJob]:
    r"""Yield one :class:`PlannedJob` per job of a plan (what ``define_jobs`` returned).

    - A source is split with :meth:`BaseSource.split` (one job per narrowed source);
      a duck-typed source with no ``split`` is one job.
    - A :class:`PlannedJob` (from :func:`job` or the ID-based plan) keeps its params
      on every job its source splits into; a list of records as its source is one
      job carrying them.
    - Any other iterable is the plan itself: each element is one job, a list of
      records (wrapped in a :class:`StaticSource` so it travels in the job payload),
      a source or a :class:`PlannedJob`. A generator is consumed lazily, so a
      ``define_jobs`` that ``yield``\ s plans an unbounded number of jobs in constant
      memory; the caller writes each job out as it arrives.
    """
    from reflowfy.sources.base import BaseSource
    from reflowfy.sources.static import StaticSource

    if isinstance(jobs, PlannedJob):
        source: Any = jobs.source
        if isinstance(source, list):
            source = StaticSource(cast(List[Any], source))
        for planned in plan_slices(source, runtime_params):
            yield PlannedJob(planned.source, jobs.params)
        return

    split = getattr(jobs, "split", None)
    if split is not None:
        for narrowed in split(runtime_params):
            yield PlannedJob(narrowed)
        return

    if isinstance(jobs, (str, bytes, dict)) or not hasattr(jobs, "__iter__"):
        # Duck-typed or custom source with no split() — one job, the source itself.
        yield PlannedJob(jobs)
        return

    for item in jobs:
        if isinstance(item, (BaseSource, PlannedJob)):
            yield from plan_slices(item, runtime_params)
        elif isinstance(item, list):
            # Empty chunk = no job, consistent with StaticSource.split().
            if item:
                # ponytail: records ride in the job payload; chunks approaching
                # Kafka's 1MB limit need a custom BaseSource that re-fetches.
                yield PlannedJob(StaticSource(cast(List[Any], item)))
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

    A ``define_destination`` that returns a list gets a :func:`fan_out` of it, routed
    like any ``@record_destination`` (so the caller always gets one destination).

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
            # A list is several destinations: fan_out(...) sends every record to each.
            destination = as_destination(
                await asyncio.to_thread(
                    pipeline.define_destination, transformed_records, runtime_params
                )
            )
        if isinstance(destination, RecordDestination) and transformed_records:
            # Choose every record's destination now, before dedup and the send: a
            # skipped record must not be claimed, and dedup hashes where each goes.
            start = time.time()
            routed = await destination.route(transformed_records, runtime_params)
            applied.append(
                AppliedStep(
                    name=destination.name,
                    duration=time.time() - start,
                    records_in=len(transformed_records),
                    records_out=len(routed.records),
                    mode=ROUTE_MODE,
                    concurrency=_route_concurrency(destination),
                    skip_reasons=routed.skip_reasons or None,
                )
            )
            transformed_records = routed.records
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
