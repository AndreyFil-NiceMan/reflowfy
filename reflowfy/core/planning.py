"""The job plan as values: ``chunk`` for ``define_jobs`` authors, ``PlannedJob`` for one
job and ``ResolvedPlan`` for one execution's plan."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


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


def _no_params() -> Dict[str, Any]:
    return {}


@dataclass(frozen=True)
class PlannedJob:
    """One job of a plan: what it reads, and the params that job alone sees.

    ``source`` is a ``BaseSource`` (split further at planning) or a list of records
    (one job carrying them). ``params`` are merged over the execution's params for
    this job only: the ID-based plan's ``current_ids``, or what :func:`reflowfy.job`
    attached. Content dedup hashes them, so two jobs with different params never
    count as duplicates of each other.
    """

    source: Any
    params: Dict[str, Any] = field(default_factory=_no_params)


@dataclass(frozen=True)
class ResolvedPlan:
    """What :meth:`AbstractPipeline.resolve` returns for one execution.

    ``params``: the validated runtime params with defaults applied; ``define_source``
    may add keys to this very dict, and they reach every job.
    ``jobs``: what ``define_jobs`` returned (a source, an iterable of jobs, or a
    generator of :class:`PlannedJob`), consumed once by the planner.
    ``execution_only``: params no single job receives (``input_ids``: a job handling
    two IDs has no use for the other million).

    A value, not state on the pipeline: the registry shares one pipeline instance
    between every execution of it in a process, and a plan stored on that instance
    was overwritten by a concurrent run of the same pipeline.
    """

    params: Dict[str, Any]
    jobs: Any
    execution_only: Tuple[str, ...] = ()

    def job_params(self, job: PlannedJob) -> Dict[str, Any]:
        """The runtime params one job runs with: the execution's, plus its own."""
        base = self.params
        if self.execution_only:
            base = {k: v for k, v in base.items() if k not in self.execution_only}
        return {**base, **job.params}
