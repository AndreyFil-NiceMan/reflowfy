"""Iterative transformation resolution and application.

`define_transformations` is re-evaluated after every applied transformation so
that runtime_params mutated by one transformation can reveal transformations
that should run later. See
docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md.
"""

import time
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, cast

from reflowfy.transformations.base import TransformationError

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


def apply_transformations_iteratively(
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

    while True:
        current = list(pipeline.define_transformations(original_records, runtime_params))
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
        start = time.time()
        try:
            # validate_input/validate_output are optional hooks; duck-typed
            # transformations may implement only `apply`.
            validate_input = getattr(transformation, "validate_input", None)
            if callable(validate_input):
                validate_input(transformed)
            transformed = transformation.apply(transformed, runtime_params)
            validate_output = getattr(transformation, "validate_output", None)
            if callable(validate_output):
                validate_output(transformed)
        except TransformationError:
            raise
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
            )
        )
        applied_count += 1

    return transformed, applied
