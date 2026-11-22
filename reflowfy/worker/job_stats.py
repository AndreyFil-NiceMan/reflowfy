"""What a worker records about one job: ``JobStats`` (stored as the job row's
``stats`` JSON) and how the applied steps land in it."""

import time
from typing import Any, Dict, List, Optional


from reflowfy.execution.job_runner import (
    log_skipped_records,
)
from reflowfy.execution.transformation_runner import AppliedStep
from reflowfy.observability import metrics
from reflowfy.observability.metrics import JobOutcome
from reflowfy.observability.logging import get_logger
from reflowfy.core.record_steps import count_skip_reason
from reflowfy.store.states import JobState

logger = get_logger("worker.executor")


class JobStats:
    """Statistics for a job execution."""

    def __init__(self) -> None:
        """Initialize job statistics."""
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.records_input = 0
        self.records_output = 0
        self.transformation_times: Dict[str, float] = {}
        self.destination_write_time: float = 0.0
        self.error: Optional[str] = None
        self.error_type: Optional[str] = None
        self.error_traceback: Optional[str] = None
        self.success = False
        self.deduplicated = False
        # Records dropped before the send because another job already sent them.
        self.records_deduplicated = 0
        self.skip_reason: Optional[str] = None
        # Records dropped with SkipRecord, and why (reason -> count, bounded).
        self.records_skipped = 0
        self.record_skip_reasons: Dict[str, int] = {}
        # Another delivery of this job already claimed it; this message was skipped.
        self.redelivered = False
        # The execution was cancelled before this job started; the message was skipped.
        self.cancelled = False
        # Distinct destinations a @destination(per_record=True) (or fan_out) sent this job's
        # records to, and how many of its optional ones failed without failing the job.
        self.destinations: Optional[int] = None
        self.destinations_failed_optional: Optional[int] = None
        # Step retries made inside this job (RetryConfig on a source, a transformation
        # or a destination): step name -> retries. Filled live by core.retry.count_retries.
        self.step_retries: Dict[str, int] = {}
        # job_retry (only for a pipeline that sets it): retries scheduled so far, and the
        # failures that led to them (type + short message, the last ERROR_HISTORY_MAX).
        self.retry_count: Optional[int] = None
        self.error_history: Optional[List[Dict[str, str]]] = None
        # This failure was not final: the job went back to pending to be retried.
        self.retry_scheduled = False
        self.retry_in_seconds: Optional[float] = None
        # The job message carries a job_retry config: a run that ends any other way than
        # a failure keeps the retry count earlier runs left (else a job that needed
        # retries looks, after the fact, like one that worked first time).
        self.job_retry_enabled = False
        # The token this delivery's claim wrote to jobs.claimed_by (None: the job has no
        # row, or the claim failed open). Not stored: it fences this delivery's writes.
        self.claim_token: Optional[str] = None
        # The job was taken from this delivery before it saved its outcome (lease
        # expired and another delivery claimed it, or it was cancelled / failed by the
        # lease sweep): the row is not ours to write.
        self.claim_lost = False

    @property
    def outcome(self) -> JobOutcome:
        """The ``reflowfy_jobs_processed_total`` status label for this delivery.

        Finer than :attr:`stored_state`: a job dropped with ``SkipJob`` is stored
        ``completed`` but counted ``skipped``, and a delivery skipped because the job
        was cancelled or already claimed stores nothing at all.
        """
        if self.cancelled:
            return JobOutcome.CANCELLED
        if self.claim_lost:
            return JobOutcome.SUPERSEDED
        if self.retry_scheduled:
            return JobOutcome.RETRY_SCHEDULED
        if self.redelivered:
            return JobOutcome.REDELIVERED
        if self.deduplicated:
            return JobOutcome.DEDUPLICATED
        if self.skip_reason is not None:
            return JobOutcome.SKIPPED
        return JobOutcome.COMPLETED if self.success else JobOutcome.FAILED

    @property
    def stored_state(self) -> str:
        """The job row's state once this run ends (see :attr:`outcome`)."""
        if self.retry_scheduled:
            return JobState.PENDING
        if self.deduplicated:
            return JobState.DEDUPLICATED
        return JobState.COMPLETED if self.success else JobState.FAILED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        duration = self.end_time - self.start_time if self.end_time else 0

        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 3),
            "records_input": self.records_input,
            "records_output": self.records_output,
            "records_deduplicated": self.records_deduplicated,
            "throughput_records_per_second": (
                round(self.records_output / duration, 2) if duration > 0 else 0
            ),
            "transformation_times": self.transformation_times,
            "destination_write_time": round(self.destination_write_time, 3),
            "error": self.error,
            "skip_reason": self.skip_reason,
            "records_skipped": self.records_skipped,
            "record_skip_reasons": self.record_skip_reasons,
            "success": self.success,
            # The exception class of a failed job (never its message): what tells a
            # RecordValidationError or PermanentDestinationError apart from other failures.
            **({"error_type": self.error_type} if self.error_type else {}),
            # Only for a @destination(per_record=True), so other jobs' stats keep their shape.
            **({"destinations": self.destinations} if self.destinations is not None else {}),
            **(
                {"destinations_failed_optional": self.destinations_failed_optional}
                if self.destinations_failed_optional is not None
                else {}
            ),
            # Only when a step retried, so other jobs' stats keep their shape: a job that
            # succeeded after retries must not look like a first-time success.
            **({"step_retries": dict(self.step_retries)} if self.step_retries else {}),
            # Only with job_retry, so other jobs' stats keep their shape.
            **({"retry_count": self.retry_count} if self.retry_count is not None else {}),
            **({"error_history": self.error_history} if self.error_history is not None else {}),
            **(
                {"retry_scheduled": True, "retry_in_seconds": self.retry_in_seconds}
                if self.retry_scheduled
                else {}
            ),
        }


def report_steps(applied: List[AppliedStep], stats: "JobStats", pipeline_name: str) -> None:
    """Time, log and count each applied step; its skipped records go on ``stats``."""
    for step in applied:
        stats.transformation_times[step.name] = round(step.duration, 3)
        logger.debug(
            "Applied transformation %s (%s, %.3fs, %d -> %d records)",
            step.name,
            step.mode if step.concurrency is None else f"{step.mode} x{step.concurrency}",
            step.duration,
            step.records_in,
            step.records_out,
            extra={
                "event": "transform_applied",
                "transformation": step.name,
                "mode": step.mode,
                "concurrency": step.concurrency,
                "records_in": step.records_in,
                "records_out": step.records_out,
            },
        )
        if step.records_skipped:
            stats.records_skipped += step.records_skipped
            for reason, times in (step.skip_reasons or {}).items():
                count_skip_reason(stats.record_skip_reasons, reason, times)
            log_skipped_records(logger, step)
            metrics.records_skipped_total.labels(
                pipeline=pipeline_name, transformation=step.name
            ).inc(step.records_skipped)
