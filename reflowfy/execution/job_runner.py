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

import json
from typing import Any, Dict, Iterator, List, Optional, Tuple

from reflowfy.core.serialization import to_json_safe
from reflowfy.execution.transformation_runner import (
    StepObserver,
    apply_transformations_iteratively,
)


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


def plan_slices(source: Any, runtime_params: Dict[str, Any]) -> Iterator[Any]:
    """Yield the per-job narrowed sources for ``source``.

    Uses :meth:`BaseSource.split` when the source provides it (the v2
    planning hook). Falls back to a single job — the source itself — for
    duck-typed or custom sources that don't implement ``split``, so local
    preview/test runs work with any source shape.

    A ``list`` is the job plan itself (what ``define_jobs`` returns): each
    element is one job, either a list of records — wrapped in a
    :class:`StaticSource` so it travels in the job payload — or a source that
    splits itself.
    """
    if isinstance(source, list):
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
                    yield StaticSource(item)
            else:
                raise TypeError(
                    "define_jobs returned a list, so each element must be one job: "
                    f"a list of records or a BaseSource, got {type(item).__name__}. "
                    "Wrap records with chunk(records, size=N), or return [records] "
                    "for a single job."
                )
        return

    split = getattr(source, "split", None)
    if split is None:
        yield source
        return
    yield from split(runtime_params)


class JobNotSerializableError(Exception):
    """A planned job cannot survive the trip to a worker.

    Raised by :func:`plan_slices_over_wire` so ``reflowfy test`` fails locally,
    with an actionable message, on the exact conditions that would otherwise
    only break at dispatch time or on the worker.
    """


def plan_slices_over_wire(source: Any, runtime_params: Dict[str, Any]) -> Iterator[Tuple[Any, int]]:
    """Yield ``(reconstructed_source, payload_bytes)`` for each planned job.

    The same slices :func:`plan_slices` yields, but each one is put through the
    exact hop the manager and worker perform — ``SourceFactory.serialize`` →
    ``json.dumps`` → ``SourceFactory.create`` — so a local test fails on a
    source that cannot travel instead of passing and failing after deploy.

    Only ``reflowfy test`` uses this. The worker and ``local_executor`` call
    :func:`plan_slices` directly: the worker has already come through the wire,
    and neither should pay for a round-trip they don't need.
    """
    from reflowfy.factories.source_factory import SourceFactory

    for sub in plan_slices(source, runtime_params):
        descriptor = SourceFactory.serialize(sub)
        try:
            payload = json.dumps(descriptor)
        except TypeError as exc:
            raise JobNotSerializableError(
                f"{descriptor['type']} config is not JSON-serializable ({exc}); it cannot "
                "travel to a worker. If this is a StaticSource from define_jobs, your "
                "records contain non-JSON values (datetime, Decimal, custom objects) — "
                "convert them before returning the job plan, e.g. "
                "`chunk(to_json_safe(rows), size=N)`."
            ) from exc

        d = json.loads(payload)
        try:
            reconstructed = SourceFactory.create(d["type"], d["config"])
        except TypeError as exc:
            raise JobNotSerializableError(
                f"{d['type']}.config must be exactly its constructor kwargs — the worker "
                f"rebuilds the source with {d['type']}(**config), which failed: {exc}. "
                f"Got config keys: {sorted(d['config'])}."
            ) from exc
        # ValueError from create() ("Unknown source type: …") already names the
        # problem and lists the registered types; let it through untouched.

        yield reconstructed, len(payload.encode("utf-8"))


def run_job_records(
    source: Any,
    pipeline: Any,
    runtime_params: Dict[str, Any],
    limit: Optional[int] = None,
    on_step: Optional[StepObserver] = None,
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

    ``on_step`` is passed straight to the transformation runner: an optional
    observer called with ``(name, records_before, records_after)`` after each
    transformation, so a caller can show or record what every step did.

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
        pipeline, records, runtime_params, on_step=on_step
    )
    destination = pipeline.define_destination(transformed_records, runtime_params)
    return records, transformed_records, applied, destination
