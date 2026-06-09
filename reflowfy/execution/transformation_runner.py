"""Iterative transformation resolution and application.

`define_transformations` is re-evaluated after every applied transformation so
that runtime_params mutated by one transformation can reveal transformations
that should run later. See
docs/superpowers/specs/2026-06-09-dynamic-transformation-resolution-design.md.
"""

import time
from typing import Any, Dict, List, Tuple

from reflowfy.transformations.base import TransformationError

DEFAULT_MAX_STEPS = 1000


def apply_transformations_iteratively(
    pipeline: Any,
    original_records: List[Any],
    runtime_params: Dict[str, Any],
    max_steps: int = DEFAULT_MAX_STEPS,
) -> Tuple[List[Any], List[Tuple[str, float]]]:
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
        ``(transformation_name, duration_seconds)`` in application order.

    Raises:
        TransformationError: If a transformation fails validation/apply, or if
            ``max_steps`` is exceeded.
    """
    transformed = original_records
    applied: List[Tuple[str, float]] = []
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
        start = time.time()
        try:
            transformation.validate_input(transformed)
            transformed = transformation.apply(transformed, runtime_params)
            transformation.validate_output(transformed)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(
                transformation_name=getattr(transformation, "name", "<unknown>"),
                message=str(exc),
                original_error=exc,
            )
        applied.append((transformation.name, time.time() - start))
        applied_count += 1

    return transformed, applied
