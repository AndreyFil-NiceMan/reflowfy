"""Unit tests: the pipeline scheduler dispatches runs asynchronously.

A scheduled trigger stages the execution record and advances the schedule
timer in the poll's transaction; once that commits, the actual jobs run on a
background thread — so a slow/long-running execution never blocks the poll loop
(and therefore never stalls the cron cadence). Overlap is allowed: a
new tick fires even while a previous run of the same pipeline is still
in flight.
"""

import threading
import time
from datetime import datetime

import reflowfy.reflow_manager.pipeline_scheduler as ps_mod
from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler
from reflowfy.reflow_manager.start import start_in_background
from reflowfy import AbstractPipeline, RuntimeParams, pipeline_registry
from typing import Any
import pytest


class _Scheduled(AbstractPipeline[RuntimeParams]):
    """Stand-in for a scheduled pipeline: the scheduler checks the name is registered."""

    def define_source(self, runtime_params: Any) -> Any:
        return []

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        return None


@pytest.fixture(autouse=True)
def _registered_pipelines() -> Any:
    names = ("due", "p", "later", "off")
    for name in names:
        type(f"_Scheduled_{name}", (_Scheduled,), {"name": name})  # registers itself
    yield
    for name in names:
        pipeline_registry.unregister(name)



class _FakeDB:
    def __init__(self):
        self.committed = False
        self.closed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        pass

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

    def run_pipeline_jobs(self, execution_id, pipeline_name, runtime_params):
        # Block until the test releases the gate, simulating a slow run.
        self._rec.gate.wait(5)
        self._rec.ran.append(execution_id)


def _factory(rec: _Recorder):
    return lambda: _FakeRunner(rec)


class _FakeSchedule:
    def __init__(self):
        self.pipeline_name = "p"
        self.schedule_name = "default"
        self.cron_expression = "*/5 * * * *"
        self.runtime_params = {}
        self.last_execution_id = None
        self.last_triggered_at = None
        self.next_run_at = None


class _FakeScheduleDB:
    def flush(self):
        pass


@pytest.fixture
def rec(monkeypatch) -> _Recorder:
    r = _Recorder(threading.Event())
    monkeypatch.setattr(
        ps_mod, "stage_execution", lambda db, eid, name, params: r.created.append(eid)
    )
    return r


def _trigger_and_start(sched: PipelineScheduler, schedule: _FakeSchedule) -> None:
    run = sched._trigger_pipeline(_FakeScheduleDB(), schedule, datetime.now())
    assert run is not None
    start_in_background(sched.pipeline_runner_factory, *run)  # what the poll does after commit


def test_trigger_does_not_block_on_a_slow_run(rec):
    sched = PipelineScheduler(pipeline_runner_factory=_factory(rec))
    schedule = _FakeSchedule()

    start = time.time()
    _trigger_and_start(sched, schedule)
    elapsed = time.time() - start

    # Returned promptly even though the run is still blocked on the gate.
    assert elapsed < 1.0, f"trigger and start blocked for {elapsed:.2f}s"
    # Execution staged synchronously; timer advanced.
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


def test_overlapping_ticks_both_run(rec):
    sched = PipelineScheduler(pipeline_runner_factory=_factory(rec))
    s1 = _FakeSchedule()
    s2 = _FakeSchedule()

    # Fire a second tick while the first is still blocked — overlap allowed.
    _trigger_and_start(sched, s1)
    _trigger_and_start(sched, s2)

    assert len(rec.created) == 2
    assert s1.last_execution_id is not None
    assert s2.last_execution_id is not None
    assert s1.last_execution_id != s2.last_execution_id

    rec.gate.set()
    deadline = time.time() + 5
    while len(rec.ran) < 2 and time.time() < deadline:
        time.sleep(0.05)
    assert len(rec.ran) == 2
