"""Iterative transformation resolution and application.

`define_transformations` is re-evaluated after every applied transformation so
that runtime_params mutated by one transformation can reveal transformations
that should run later. See
docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md.

The loop is async so a transformation can await (an ``AsyncTransformation``, or
a ``RecordTransformation`` running its records concurrently) on the worker's
event loop. Synchronous user code — plain ``apply``, ``define_transformations`` —
runs in a thread, so it never stalls that loop (and with it the Kafka heartbeat).
"""

import asyncio
import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, cast

from reflowfy.core.abstract_pipeline import AbstractPipeline
from reflowfy.core.async_utils import call_hook
from reflowfy.core.exceptions import SkipJob, SkipRecord
from reflowfy.core.record_validation import describe_invalid, find_invalid_records
from reflowfy.core.retry import RetryConfig, retry_async
from reflowfy.observability.logging import get_logger
from reflowfy.transformations.output_type import step_output_type
from reflowfy.transformations.base import (
    AsyncTransformation,
    RecordFailure,
    RecordResults,
    RecordTransformation,
    SyncRecordTransformation,
    as_transformations,
)
from reflowfy.core.exceptions import StepOutputError, TransformationError

logger = get_logger("execution.transformation_runner")

DEFAULT_MAX_STEPS = 1000


class StepMode:
    """Every value of ``AppliedStep.mode``: how a reported step ran.

    Plain strings, not an Enum: they are logged, printed and written to ``--json``.
    """

    SYNC = "sync"  # a sync transformation, run in a thread
    ASYNC = "async"  # a whole-batch async transformation
    PER_RECORD = "per_record"  # @transformation(per_record=True): one call per record
    SKIPPED = "skipped"  # should_apply_transformation declined it: it did not run
    VALIDATE = "validate"  # the pipeline's record_type check (job_runner.check_record_type)
    ROUTE = "route"  # a @destination(per_record=True) choosing each record's destination


SKIPPED_MODE = StepMode.SKIPPED
"""``AppliedStep.mode`` of a step ``should_apply_transformation`` declined."""


def step_name(transformation: Any) -> str:
    """A step's name for reports and errors; ``<unknown>`` for an object without one."""
    return str(getattr(transformation, "name", "<unknown>"))


class AppliedStep(NamedTuple):
    """One applied transformation: what it was, how long it took, what it did.

    ``records_in``/``records_out`` are the counts either side of ``apply``, so a
    step that silently empties the batch is visible rather than inferred. Field
    order keeps ``name`` and ``duration`` first; callers should prefer attribute
    access (``step.name``) over positional unpacking.
    """

    name: str
    duration: float
    records_in: int
    records_out: int
    mode: str = StepMode.SYNC
    """How the step ran: one of :class:`StepMode`.

    ``skipped``: the pipeline's ``should_apply_transformation`` declined it, so it did
    not run (``records_in == records_out``; ``duration`` is the hook's time)."""
    concurrency: Optional[int] = None
    """Records in flight at once, for a ``"per_record"`` step."""
    skip_reasons: Optional[Dict[str, int]] = None
    """``SkipRecord`` reason -> records dropped for it, for a ``"per_record"`` step."""

    @property
    def records_skipped(self) -> int:
        """Records this step dropped with ``SkipRecord``."""
        return sum((self.skip_reasons or {}).values())

    @property
    def delta(self) -> int:
        """Records added (positive) or dropped (negative) by this step."""
        return self.records_out - self.records_in


def find_culprit_record(
    exc: Exception, records: List[Any], scan_limit: int = 10_000
) -> Tuple[Optional[int], Optional[Any]]:
    """Best-effort: point at the record that made ``exc`` happen.

    Deliberately does NOT re-run the transformation. Re-running user code to
    bisect a failure would duplicate any side effects it already had, which is
    unacceptable on the worker path. Instead this inspects the exception and
    looks for a record that matches its complaint.

    Only ``KeyError`` is decided confidently — it names the missing key, and it
    is far and away the most common transformation failure. Everything else
    returns ``(None, None)``; the caller falls back to showing a sample.

    Returns ``(index, record)``, or ``(None, None)`` if no record is implicated.
    """
    if not isinstance(exc, KeyError) or not exc.args:
        return None, None
    missing = exc.args[0]
    for i, record in enumerate(records[:scan_limit]):
        # A non-mapping record cannot be the source of a missing-key error here.
        if isinstance(record, dict) and missing not in cast(Dict[Any, Any], record):
            # Index back into `records` so the returned value keeps its original
            # type rather than the narrowed dict.
            return i, records[i]
    return None, None


def _run_sync_step(transformation: Any, records: List[Any], runtime_params: Dict[str, Any]) -> Any:
    # validate_input/validate_output are optional hooks; duck-typed
    # transformations may implement only `apply`.
    validate_input = getattr(transformation, "validate_input", None)
    if callable(validate_input):
        validate_input(records)
    result = transformation.apply(records, runtime_params)
    validate_output = getattr(transformation, "validate_output", None)
    if callable(validate_output):
        validate_output(result)
    return result


async def _run_async_step(
    transformation: AsyncTransformation, records: List[Any], runtime_params: Dict[str, Any]
) -> RecordResults:
    transformation.validate_input(records)
    if isinstance(transformation, RecordTransformation):
        result = await transformation.apply_records(records, runtime_params)
    else:
        result = RecordResults(await transformation.apply_async(records, runtime_params), {})
    transformation.validate_output(result.records)
    return result


def _step_retry(transformation: Any) -> Optional[RetryConfig]:
    """A step's ``retry``; a duck-typed step (no ``BaseTransformation``) has none."""
    retry = getattr(transformation, "retry", None)
    return retry if isinstance(retry, RetryConfig) else None


def _check_step_output(
    transformation: Any, records: List[Any], step_index: int, applied: List[AppliedStep]
) -> None:
    """Fail the job if a step's records do not match the type it promises to return."""
    output_type = step_output_type(transformation)
    if output_type is None or not records:
        return
    invalid = find_invalid_records(output_type, records)
    if not invalid:
        return
    culprit_index = min(invalid)
    raise StepOutputError(
        transformation_name=step_name(transformation),
        message=f"returned {describe_invalid(invalid, len(records), output_type)}",
        step_index=step_index,
        culprit_index=culprit_index,
        culprit_record=records[culprit_index],
        applied_steps=list(applied),
    )


def _decides_steps(pipeline: Any) -> bool:
    """Whether ``pipeline`` overrides ``should_apply_transformation``.

    The default always says yes; not calling it saves a thread hop per step.
    """
    hook = getattr(cast(object, type(pipeline)), "should_apply_transformation", None)
    if hook is None:
        return False
    return hook is not AbstractPipeline.should_apply_transformation


async def _should_apply(
    pipeline: Any,
    transformation: Any,
    records: List[Any],
    runtime_params: Dict[str, Any],
    step_index: int,
    applied: List[AppliedStep],
) -> bool:
    """Ask the pipeline whether to run this step; its failure fails the job, naming the step."""
    try:
        # User code: in a thread like a sync apply, or awaited if it is an async def.
        return bool(
            await call_hook(
                pipeline.should_apply_transformation, transformation, runtime_params, records
            )
        )
    except SkipJob:
        raise
    except Exception as exc:
        raise TransformationError(
            transformation_name=step_name(transformation),
            message=f"should_apply_transformation raised {type(exc).__name__}: {exc}",
            original_error=exc,
            step_index=step_index,
            records_in=len(records),
            applied_steps=list(applied),
        ) from exc


async def apply_transformations_iteratively_async(
    pipeline: Any,
    original_records: List[Any],
    runtime_params: Dict[str, Any],
    max_steps: int = DEFAULT_MAX_STEPS,
) -> Tuple[List[Any], List[AppliedStep]]:
    """Apply a pipeline's transformations, re-resolving the list after each step.

    ``define_transformations`` is always called with the ORIGINAL pre-transformation
    records; only ``runtime_params`` changes between re-resolutions. The list must
    be append-only: re-resolution may only grow it, and already-applied steps
    (positions < applied_count) are never re-applied or un-applied.

    Steps run one after another whatever their kind; only the records inside one
    ``RecordTransformation`` step run concurrently.

    Before each step the pipeline's ``should_apply_transformation(transformation,
    runtime_params, records)`` is asked (``records`` as they are at that point). A
    declined step keeps its position — it counts as applied, so re-resolution never
    offers it again — and is reported with ``mode="skipped"``.

    Args:
        pipeline: A resolved pipeline exposing ``name`` and ``define_transformations``.
        original_records: The pre-transformation records for this job/batch.
        runtime_params: The shared, mutable runtime params dict. Mutations made by a
            transformation's ``apply`` are visible to the next re-resolution.
        max_steps: Safety cap on how many transformations may be applied; protects
            against a ``define_transformations`` that appends without bound.

    Returns:
        ``(transformed_records, applied)`` where ``applied`` is a list of
        :class:`AppliedStep` in application order.

    Raises:
        TransformationError: If a transformation fails validation/apply, or if
            ``max_steps`` is exceeded.
    """
    applied: List[AppliedStep] = []
    try:
        return await _apply_steps(pipeline, original_records, runtime_params, max_steps, applied)
    except SkipJob as exc:
        # The job is dropped, but what ran before it did happen: hand those steps
        # (and their SkipRecord reasons) to the caller, which reports them beside
        # the drop instead of only the drop.
        exc.applied_steps = list(applied)
        raise


def _check_append_only(pipeline: Any, current: List[Any], applied: List[AppliedStep]) -> None:
    """Re-resolution may only grow the list: a step that already ran must still be at
    its position, under the same name.

    The runner applies ``current[len(applied)]`` next, so a list that dropped or
    replaced an earlier step used to shift every later one silently: a step ran twice
    or never, depending on where the change was.
    """
    if len(current) < len(applied):
        raise TransformationError(
            transformation_name=getattr(pipeline, "name", "<unknown>"),
            message=(
                f"define_transformations returned {len(current)} step(s) after "
                f"{len(applied)} had run; the list may only grow (append steps, don't "
                "remove them)"
            ),
        )
    for index, step in enumerate(applied):
        now = step_name(current[index])
        if now != step.name:
            raise TransformationError(
                transformation_name=getattr(pipeline, "name", "<unknown>"),
                message=(
                    f"define_transformations changed step {index + 1} from {step.name!r} to "
                    f"{now!r} after it had run; the list may only grow (append steps, "
                    "don't replace or reorder them)"
                ),
            )


async def _apply_steps(
    pipeline: Any,
    original_records: List[Any],
    runtime_params: Dict[str, Any],
    max_steps: int,
    applied: List[AppliedStep],
) -> Tuple[List[Any], List[AppliedStep]]:
    transformed = original_records
    applied_count = 0
    decides_steps = _decides_steps(pipeline)

    async def resolve() -> List[Any]:
        return as_transformations(
            await call_hook(pipeline.define_transformations, original_records, runtime_params)
        )

    while True:
        current = await resolve()
        _check_append_only(pipeline, current, applied)
        if len(current) <= applied_count:
            break

        if applied_count >= max_steps:
            raise TransformationError(
                transformation_name=getattr(pipeline, "name", "<unknown>"),
                message=(
                    f"Exceeded max_steps={max_steps} while resolving transformations; "
                    "define_transformations appears to append without bound."
                ),
            )

        transformation = current[applied_count]
        records_in = len(transformed)
        mode = StepMode.SYNC
        concurrency: Optional[int] = None
        skip_reasons: Optional[Dict[str, int]] = None
        start = time.time()
        if decides_steps and not await _should_apply(
            pipeline, transformation, transformed, runtime_params, applied_count, applied
        ):
            logger.info(
                "Transformation %s not applied: should_apply_transformation declined it",
                step_name(transformation),
                extra={
                    "event": "transform_declined",
                    "transformation": step_name(transformation),
                    "records_in": records_in,
                },
            )
            applied.append(
                AppliedStep(
                    name=step_name(transformation),
                    duration=time.time() - start,
                    records_in=records_in,
                    records_out=records_in,
                    mode=SKIPPED_MODE,
                )
            )
            applied_count += 1
            continue
        try:
            if isinstance(transformation, RecordTransformation):
                # A sync per-record step runs its records one at a time in a thread:
                # no concurrency to report.
                mode = StepMode.PER_RECORD
                if not isinstance(transformation, SyncRecordTransformation):
                    concurrency = transformation.effective_concurrency()
                logger.debug(
                    "Transformation %s: %d records, %s at a time",
                    step_name(transformation),
                    records_in,
                    concurrency or "one",
                    extra={
                        "event": "transform_per_record_start",
                        "transformation": step_name(transformation),
                        "records_in": records_in,
                        "concurrency": concurrency,
                    },
                )
            elif isinstance(transformation, AsyncTransformation):
                mode = StepMode.ASYNC
            # A per-record step retries each record itself; a batch step is
            # retried here, on the batch it was given.
            retry = None if mode == StepMode.PER_RECORD else _step_retry(transformation)
            step_input = transformed
            if isinstance(transformation, AsyncTransformation):
                async_step = transformation
                result = await retry_async(
                    lambda: _run_async_step(async_step, step_input, runtime_params),
                    retry,
                    step=step_name(transformation),
                )
                transformed = result.records
                if mode == StepMode.PER_RECORD:
                    skip_reasons = result.skip_reasons
            else:
                sync_step = transformation
                transformed = await retry_async(
                    lambda: asyncio.to_thread(
                        _run_sync_step, sync_step, step_input, runtime_params
                    ),
                    retry,
                    step=step_name(transformation),
                )
            if getattr(pipeline, "validate_steps", False):
                await asyncio.to_thread(
                    _check_step_output, transformation, transformed, applied_count, applied
                )
        except SkipJob as exc:
            if exc.step_skip_reasons:
                # A per-record step stopped part way: the records it had already
                # skipped are reported; the job is dropped, so nothing came out.
                applied.append(
                    AppliedStep(
                        name=step_name(transformation),
                        duration=time.time() - start,
                        records_in=records_in,
                        records_out=0,
                        mode=mode,
                        concurrency=concurrency,
                        skip_reasons=dict(exc.step_skip_reasons),
                    )
                )
            raise
        except TransformationError:
            raise
        except SkipRecord as exc:
            # Only a per-record step knows which record it is working on.
            raise TransformationError(
                transformation_name=step_name(transformation),
                message=(
                    f"SkipRecord({str(exc)!r}) raised from a {mode} transformation; it only "
                    "works in a per-record one (@transformation(per_record=True)). Filter the record "
                    "out of the list instead, or raise SkipJob to drop the whole job."
                ),
                original_error=exc,
                step_index=applied_count,
                records_in=records_in,
                applied_steps=list(applied),
            ) from exc
        except RecordFailure as exc:
            # A per-record step knows exactly which record broke; no guessing.
            raise TransformationError(
                transformation_name=step_name(transformation),
                message=str(exc),
                original_error=exc.error,
                step_index=applied_count,
                records_in=records_in,
                culprit_index=exc.index,
                culprit_record=exc.record,
                applied_steps=list(applied),
            ) from exc.error
        except Exception as exc:
            culprit_index, culprit = find_culprit_record(exc, transformed)
            raise TransformationError(
                transformation_name=step_name(transformation),
                # Name the type: bare str(KeyError) is just "'user_id'", which
                # tells the reader nothing about what went wrong.
                message=f"{type(exc).__name__}: {exc}",
                original_error=exc,
                step_index=applied_count,
                records_in=records_in,
                culprit_index=culprit_index,
                culprit_record=culprit,
                # The steps that already succeeded, so a reporter can show the
                # whole chain up to the break rather than just the break.
                applied_steps=list(applied),
            )
        applied.append(
            AppliedStep(
                name=step_name(transformation),
                duration=time.time() - start,
                records_in=records_in,
                records_out=len(transformed),
                mode=mode,
                concurrency=concurrency,
                skip_reasons=skip_reasons,
            )
        )
        applied_count += 1

    return transformed, applied
