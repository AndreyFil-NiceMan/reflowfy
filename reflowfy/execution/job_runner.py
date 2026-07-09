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

from typing import Any, Dict, Iterator, List, Optional, Tuple

from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.transformation_runner import apply_transformations_iteratively


def plan_slices(source: Any, runtime_params: Dict[str, Any]) -> Iterator[Any]:
    """Yield the per-job narrowed sources for ``source``.

    Uses :meth:`BaseSource.split` when the source provides it (the v2
    planning hook). Falls back to a single job — the source itself — for
    duck-typed or custom sources that don't implement ``split``, so local
    preview/test runs work with any source shape.
    """
    split = getattr(source, "split", None)
    if split is None:
        yield source
        return
    yield from split(runtime_params)


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
    destination = pipeline.define_destination(transformed_records, runtime_params)
    return records, transformed_records, applied, destination
