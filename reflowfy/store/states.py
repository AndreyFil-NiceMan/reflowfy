"""The states a job row and an execution row move through.

Plain string constants rather than an ``Enum``: the values are what the columns hold
and what the routes return, and a ``(str, Enum)`` formats as ``JobState.PENDING`` on
Python 3.11+ but as ``pending`` on 3.10, which this package also supports.
"""

from typing import Mapping, Tuple


class JobState:
    """``pending → dispatched → running → completed | failed | deduplicated``; a pending
    or dispatched job can be ``cancelled``; ``running → pending`` is a scheduled retry
    or an expired lease."""

    PENDING = "pending"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEDUPLICATED = "deduplicated"
    CANCELLED = "cancelled"

    ALL: Tuple[str, ...] = (
        PENDING,
        DISPATCHED,
        RUNNING,
        COMPLETED,
        FAILED,
        DEDUPLICATED,
        CANCELLED,
    )
    # No worker has started it: a worker may claim it, a cancel may drop it.
    UNSTARTED: Tuple[str, ...] = (PENDING, DISPATCHED)
    IN_FLIGHT: Tuple[str, ...] = (DISPATCHED, RUNNING)
    # A worker reported an outcome.
    FINISHED: Tuple[str, ...] = (COMPLETED, FAILED, DEDUPLICATED)


class ExecutionState:
    """``pending → running ⇄ paused → completed | failed | cancelled``."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    ALL: Tuple[str, ...] = (PENDING, RUNNING, PAUSED, COMPLETED, FAILED, CANCELLED)
    TERMINAL: Tuple[str, ...] = (COMPLETED, FAILED, CANCELLED)
    ACTIVE: Tuple[str, ...] = (PENDING, RUNNING, PAUSED)

    # The states each state may be entered from. ``ExecutionManager.update_execution_state``
    # writes ``UPDATE … WHERE state IN FROM[to]`` (compare-and-set), so of two concurrent
    # transitions only one that is still legal lands: a cancel is never overwritten by the
    # planner's ``running``, a finished run is never paused. Leaving a terminal state is
    # ``reopen_execution`` (``/retry-failed``) only, and ``pending`` is only created.
    FROM: Mapping[str, Tuple[str, ...]] = {
        PENDING: (),
        RUNNING: (PENDING, PAUSED),
        PAUSED: (PENDING, RUNNING),
        COMPLETED: (PENDING, RUNNING),
        FAILED: (PENDING, RUNNING, PAUSED),
        CANCELLED: (PENDING, RUNNING, PAUSED),
    }


def jobs_dispatched(counts: Mapping[str, int]) -> int:
    """Jobs that have left the manager: sent, running or finished (from a count per
    job state)."""
    return sum(counts.get(state, 0) for state in JobState.IN_FLIGHT + JobState.FINISHED)
