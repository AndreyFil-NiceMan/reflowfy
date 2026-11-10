"""How long a job row may go without news before the manager acts on it.

The worker's heartbeat and the manager's running lease are two halves of one rule:
a running job whose ``updated_at`` is older than the lease is taken for dead and
requeued. They used to be read in two packages with nothing tying them together, so
a lease set below the heartbeat requeued every long job while its worker was alive.
"""

import logging
import os
from dataclasses import dataclass

# A lease shorter than this many heartbeats requeues live jobs after a missed beat.
MIN_BEATS_PER_LEASE = 3


@dataclass(frozen=True)
class JobLeases:
    """``JOB_HEARTBEAT_SECONDS`` (worker, default 30), ``JOB_RUNNING_LEASE_SECONDS``
    (300), ``JOB_DISPATCH_LEASE_SECONDS`` (3600: dispatched, never claimed → re-sent)
    and ``JOB_MAX_ATTEMPTS`` (3 claims before a job whose worker keeps vanishing
    fails)."""

    heartbeat_seconds: float = 30.0
    running_lease_seconds: float = 300.0
    dispatch_lease_seconds: float = 3600.0
    max_attempts: int = 3

    @classmethod
    def from_env(cls) -> "JobLeases":
        return cls(
            heartbeat_seconds=float(os.getenv("JOB_HEARTBEAT_SECONDS", "30")),
            running_lease_seconds=float(os.getenv("JOB_RUNNING_LEASE_SECONDS", "300")),
            dispatch_lease_seconds=float(os.getenv("JOB_DISPATCH_LEASE_SECONDS", "3600")),
            max_attempts=int(os.getenv("JOB_MAX_ATTEMPTS", "3")),
        )

    def warn_if_inconsistent(self, logger: logging.Logger) -> bool:
        """Log ``job_lease_too_short`` when the running lease leaves room for fewer than
        ``MIN_BEATS_PER_LEASE`` heartbeats. Both services call it at startup, since
        each sees only its own environment."""
        if self.running_lease_seconds >= self.heartbeat_seconds * MIN_BEATS_PER_LEASE:
            return False
        logger.warning(
            "JOB_RUNNING_LEASE_SECONDS (%ss) is less than %d x JOB_HEARTBEAT_SECONDS (%ss): "
            "running jobs whose worker is alive will be requeued and run twice",
            self.running_lease_seconds,
            MIN_BEATS_PER_LEASE,
            self.heartbeat_seconds,
            extra={"event": "job_lease_too_short"},
        )
        return True
