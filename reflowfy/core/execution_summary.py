"""What a finished execution did — passed to a pipeline's lifecycle hooks.

``on_execution_complete(summary, params)`` / ``on_execution_failed`` /
``on_execution_cancelled`` receive one, and a ``/run`` ``callback_url`` gets it as JSON.
"""

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ExecutionSummary:
    """One execution, once it reached a terminal state.

    Attributes:
        execution_id: The execution.
        pipeline_name: Its pipeline.
        state: ``completed``, ``failed`` or ``cancelled``.
        jobs_total: Jobs planned.
        jobs_completed: Jobs that finished (dropped ones included).
        jobs_failed: Jobs that failed (after their retries, if any).
        jobs_deduplicated: Jobs whose every record had already been sent.
        jobs_cancelled: Jobs that never ran because the execution was cancelled.
        jobs_skipped: Completed jobs the pipeline dropped (``SkipJob``).
        records_in: Records fetched by the jobs that finished.
        records_out: Records sent.
        records_skipped: Records dropped with a reason (``SkipRecord``, invalid records).
        records_deduplicated: Records not sent because another job already had.
        retries: Job retries scheduled by ``job_retry`` over the whole execution.
        duration_seconds: From the execution's creation to its end.
        created_at: ISO time the execution was created.
        finished_at: ISO time it ended.
        error_message: The execution's own error (why it failed or was cancelled), if any.
        first_errors: Up to five failed jobs' error messages, shortened.
        parent_execution_id: The execution whose hook started this one, if any.
    """

    execution_id: str
    pipeline_name: str
    state: str
    jobs_total: int = 0
    jobs_completed: int = 0
    jobs_failed: int = 0
    jobs_deduplicated: int = 0
    jobs_cancelled: int = 0
    jobs_skipped: int = 0
    records_in: int = 0
    records_out: int = 0
    records_skipped: int = 0
    records_deduplicated: int = 0
    retries: int = 0
    duration_seconds: Optional[float] = None
    created_at: Optional[str] = None
    finished_at: Optional[str] = None
    error_message: Optional[str] = None
    first_errors: List[str] = field(default_factory=lambda: [])
    parent_execution_id: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        """True for a completed execution."""
        return self.state == "completed"

    def to_dict(self) -> Dict[str, Any]:
        """JSON-ready form (the body POSTed to a ``callback_url``)."""
        return asdict(self)
