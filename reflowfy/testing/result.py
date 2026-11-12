"""What a pipeline did when it ran under test."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from reflowfy.execution.transformation_runner import AppliedStep
from reflowfy.transformations.base import count_skip_reason


@dataclass
class JobResult:
    """One job: what it fetched, what each step did, and what it would send."""

    index: int
    params: Dict[str, Any] = field(default_factory=lambda: {})
    """The runtime_params the job ran with (after its steps wrote to them)."""
    job_params: Dict[str, Any] = field(default_factory=lambda: {})
    """The params the plan attached to this job alone (``job(...)``, ``current_ids``)."""
    fetched: List[Any] = field(default_factory=lambda: [])
    records: List[Any] = field(default_factory=lambda: [])
    """The records after every transformation."""
    steps: List[AppliedStep] = field(default_factory=lambda: [])
    destination: Any = None
    """What ``define_destination`` returned, or ``None`` for a job with no records."""
    sent: bool = False
    skip_reason: Optional[str] = None
    """Why the pipeline dropped the job with ``SkipJob``, if it did."""
    error: Optional[BaseException] = None


@dataclass
class PipelineResult:
    """Everything a run produced. Nothing here was written to a real system unless
    the test asked for ``destination="real"``."""

    pipeline: str
    params: Dict[str, Any] = field(default_factory=lambda: {})
    jobs: List[JobResult] = field(default_factory=lambda: [])
    error: Optional[BaseException] = None
    """The failure that stopped the run: a job's, or the planning's."""

    @property
    def ok(self) -> bool:
        """True when nothing failed."""
        return self.error is None

    @property
    def sent(self) -> List[Any]:
        """Every record sent to the destination, across jobs, in order."""
        return [record for job in self.jobs if job.sent for record in job.records]

    @property
    def records(self) -> List[Any]:
        """Every transformed record, sent or not (``send=False`` sends none)."""
        return [record for job in self.jobs for record in job.records]

    @property
    def fetched(self) -> int:
        """How many records the jobs fetched."""
        return sum(len(job.fetched) for job in self.jobs)

    @property
    def steps(self) -> List[AppliedStep]:
        """Every applied step, across jobs."""
        return [step for job in self.jobs for step in job.steps]

    @property
    def skip_reasons(self) -> Dict[str, int]:
        """``SkipRecord`` reason (or ``invalid …`` from ``record_type``) -> records dropped."""
        reasons: Dict[str, int] = {}
        for step in self.steps:
            for reason, count in (step.skip_reasons or {}).items():
                count_skip_reason(reasons, reason, count)
        return reasons

    @property
    def records_skipped(self) -> int:
        """Records dropped with a reason."""
        return sum(self.skip_reasons.values())

    @property
    def jobs_dropped(self) -> List[str]:
        """The reason of every job the pipeline dropped with ``SkipJob``."""
        return [job.skip_reason for job in self.jobs if job.skip_reason is not None]

    @property
    def destinations(self) -> List[Any]:
        """The destination each job resolved (jobs with no records have none)."""
        return [job.destination for job in self.jobs if job.destination is not None]

    def raise_for_error(self) -> "PipelineResult":
        """Raise the failure that stopped the run, if there was one."""
        if self.error is not None:
            raise self.error
        return self

    def __repr__(self) -> str:
        state = "ok" if self.ok else f"failed: {type(self.error).__name__}: {self.error}"
        return (
            f"<PipelineResult {self.pipeline} {state}; {len(self.jobs)} job(s), "
            f"{self.fetched} fetched, {len(self.sent)} sent, {self.records_skipped} skipped>"
        )
