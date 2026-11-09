"""Helpers for writing a job plan (``AbstractPipeline.define_jobs``)."""

from typing import Any, List


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
