"""Unit tests: the pipeline scheduler dispatches runs asynchronously.

A scheduled trigger must create the execution record and advance the
schedule timer synchronously, then run the actual jobs on a background
thread — so a slow/long-running execution never blocks the poll loop
(and therefore never stalls the cron cadence). Overlap is allowed: a
new tick fires even while a previous run of the same pipeline is still
in flight.
"""

import threading
import time
from datetime import datetime

from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler


class _FakeDB:
    def __init__(self):
        self.committed = False
        self.closed = False

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class _Recorder:
    """Shared sink so assertions see every fake runner instance's activity."""

    def __init__(self, gate: threading.Event):
        self.gate = gate
        self.created: list = []
        self.ran: list = []
        self.failed: list = []


class _FakeExecMgr:
    def __init__(self, rec: _Recorder):
        self._rec = rec
        self.db = _FakeDB()

    def create_execution(self, execution_id, pipeline_name, runtime_params):
        self._rec.created.append(execution_id)

    def update_execution_state(self, execution_id, state, error_message=None):
        self._rec.failed.append((execution_id, state))


class _FakeRunner:
    def __init__(self, rec: _Recorder):
        self._rec = rec
        self.execution_manager = _FakeExecMgr(rec)

    def _run_pipeline_jobs(self, execution_id, pipeline_name, runtime_params):
        # Block until the test releases the gate, simulating a slow run.
        self._rec.gate.wait(5)
        self._rec.ran.append(execution_id)


def _factory(rec: _Recorder):
    return lambda: _FakeRunner(rec)


class _FakeSchedule:
    def __init__(self):
        self.pipeline_name = "p"
        self.cron_expression = "*/5 * * * *"
        self.last_execution_id = None
        self.last_triggered_at = None
        self.next_run_at = None


class _FakeScheduleDB:
    def flush(self):
        pass


def test_trigger_does_not_block_on_a_slow_run():
    rec = _Recorder(threading.Event())
    sched = PipelineScheduler(pipeline_runner_factory=_factory(rec))
    schedule = _FakeSchedule()
    db = _FakeScheduleDB()

    start = time.time()
    sched._trigger_pipeline(db, schedule, datetime.now())
    elapsed = time.time() - start

    # Returned promptly even though the run is still blocked on the gate.
    assert elapsed < 1.0, f"_trigger_pipeline blocked for {elapsed:.2f}s"
    # Execution created synchronously; timer advanced.
    assert rec.created == [schedule.last_execution_id]
    assert schedule.last_execution_id is not None
    assert schedule.next_run_at is not None
    assert schedule.last_triggered_at is not None
    # The job has NOT finished yet (still blocked).
    assert rec.ran == []

    # Release the gate — the background thread completes the run.
    rec.gate.set()
    deadline = time.time() + 5
    while rec.ran == [] and time.time() < deadline:
        time.sleep(0.05)
    assert rec.ran == [schedule.last_execution_id]


def test_overlapping_ticks_both_run():
    rec = _Recorder(threading.Event())
    sched = PipelineScheduler(pipeline_runner_factory=_factory(rec))
    s1 = _FakeSchedule()
    s2 = _FakeSchedule()
    db = _FakeScheduleDB()

    # Fire a second tick while the first is still blocked — overlap allowed.
    sched._trigger_pipeline(db, s1, datetime.now())
    sched._trigger_pipeline(db, s2, datetime.now())

    assert len(rec.created) == 2
    assert s1.last_execution_id is not None
    assert s2.last_execution_id is not None
    assert s1.last_execution_id != s2.last_execution_id

    rec.gate.set()
    deadline = time.time() + 5
    while len(rec.ran) < 2 and time.time() < deadline:
        time.sleep(0.05)
    assert len(rec.ran) == 2


def test_dispatch_failure_marks_execution_failed():
    rec = _Recorder(threading.Event())
    rec.gate.set()  # don't block; we want the run to proceed then fail

    class _FailingRunner(_FakeRunner):
        def _run_pipeline_jobs(self, execution_id, pipeline_name, runtime_params):
            raise RuntimeError("boom")

    sched = PipelineScheduler(pipeline_runner_factory=lambda: _FailingRunner(rec))
    schedule = _FakeSchedule()
    db = _FakeScheduleDB()

    sched._trigger_pipeline(db, schedule, datetime.now())

    deadline = time.time() + 5
    while rec.failed == [] and time.time() < deadline:
        time.sleep(0.05)
    assert rec.failed == [(schedule.last_execution_id, "failed")]
