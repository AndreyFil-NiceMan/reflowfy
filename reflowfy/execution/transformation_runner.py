"""Iterative transformation resolution and application.

`define_transformations` is re-evaluated after every applied transformation so
that runtime_params mutated by one transformation can reveal transformations
that should run later. See
docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md.

The loop is async so a transformation can await (an ``AsyncTransformation``, or
a ``RecordTransformation`` running its records concurrently) on the worker's
event loop. Synchronous user code — plain ``apply``, ``define_transformations`` —
runs in a thread, so it never stalls that loop (and with it the Kafka heartbeat).
The sync :func:`apply_transformations_iteratively` drives the same loop.
"""

import asyncio
import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, cast

from reflowfy.core.async_utils import run_coroutine_sync
from reflowfy.core.exceptions import SkipJob, SkipRecord
from reflowfy.observability.logging import get_logger
from reflowfy.transformations.base import (
    AsyncTransformation,
    RecordFailure,
    RecordResults,
    RecordTransformation,
    TransformationError,
)

logger = get_logger("execution.transformation_runner")

DEFAULT_MAX_STEPS = 1000


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
    mode: str = "sync"
    """How the step ran: ``"sync"`` (in a thread), ``"async"`` or ``"per_record"``."""
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


def apply_transformations_iteratively(
    pipeline: Any,
    original_records: List[Any],
    runtime_params: Dict[str, Any],
    max_steps: int = DEFAULT_MAX_STEPS,
) -> Tuple[List[Any], List[AppliedStep]]:
    """Synchronous form of :func:`apply_transformations_iteratively_async`."""
    return run_coroutine_sync(
        apply_transformations_iteratively_async(
            pipeline, original_records, runtime_params, max_steps=max_steps
        )
    )


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
    transformed = original_records
    applied: List[AppliedStep] = []
    applied_count = 0

    def resolve() -> List[Any]:
        return list(pipeline.define_transformations(original_records, runtime_params))

    while True:
        current = await asyncio.to_thread(resolve)
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
        mode = "sync"
        concurrency: Optional[int] = None
        skip_reasons: Optional[Dict[str, int]] = None
        start = time.time()
        try:
            if isinstance(transformation, RecordTransformation):
                mode, concurrency = "per_record", transformation.effective_concurrency()
                logger.debug(
                    "Transformation %s: %d records, %d at a time",
                    transformation.name,
                    records_in,
                    concurrency,
                    extra={
                        "event": "transform_per_record_start",
                        "transformation": transformation.name,
                        "records_in": records_in,
                        "concurrency": concurrency,
                    },
                )
            elif isinstance(transformation, AsyncTransformation):
                mode = "async"
            if isinstance(transformation, AsyncTransformation):
                result = await _run_async_step(transformation, transformed, runtime_params)
                transformed = result.records
                if mode == "per_record":
                    skip_reasons = result.skip_reasons
            else:
                transformed = await asyncio.to_thread(
                    _run_sync_step, transformation, transformed, runtime_params
                )
        except (TransformationError, SkipJob):
            raise
        except SkipRecord as exc:
            # Only a per-record step knows which record it is working on.
            raise TransformationError(
                transformation_name=getattr(transformation, "name", "<unknown>"),
                message=(
                    f"SkipRecord({str(exc)!r}) raised from a {mode} transformation; it only "
                    "works in a per-record one (@record_transformation). Filter the record "
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
                transformation_name=getattr(transformation, "name", "<unknown>"),
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
                transformation_name=getattr(transformation, "name", "<unknown>"),
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
                name=transformation.name,
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
