"""PipelineRunner.start_pipeline: create the execution now, drive it on its own thread.

The DLQ used to drive each run to the end inside its poll, holding the poll's
``FOR UPDATE`` row locks: one long run stalled every other due DLQ job.
"""

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import reflowfy.reflow_manager.dlq_scheduler as dlq_mod
from reflowfy import AbstractPipeline, RuntimeParams, pipeline_registry
from reflowfy.reflow_manager.dlq_scheduler import DLQScheduler
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.store.models import Base, DLQJob, Execution
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner

PIPE = "start_pipeline_bg_test"


class _Pipeline(AbstractPipeline[RuntimeParams]):
    name = PIPE

    def define_parameters(self) -> List[Any]:
        from reflowfy.core.abstract_pipeline import PipelineParameter

        return [PipelineParameter(name="day", param_type=str, required=True)]

    def define_source(self, runtime_params: Any) -> Any:
        return [[{"id": 1}]]

    def define_transformations(self, records: Any, runtime_params: Any) -> List[Any]:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        raise AssertionError("not reached: run_pipeline_jobs is faked")


@pytest.fixture
def sessions(monkeypatch) -> Iterator[sessionmaker[Session]]:
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(dlq_mod, "SessionLocal", factory)
    yield factory
    engine.dispose()


@pytest.fixture(autouse=True)
def registered() -> Iterator[None]:
    pipeline_registry.register(_Pipeline())
    yield
    pipeline_registry.unregister(PIPE)


class _Gate:
    """Holds run_pipeline_jobs open until released; records what ran."""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.started = threading.Event()
        self.finished = threading.Event()
        self.error: Exception | None = None
        self.ran: List[Dict[str, Any]] = []


@pytest.fixture
def gate(monkeypatch) -> _Gate:
    g = _Gate()

    def run_pipeline_jobs(self: PipelineRunner, execution_id: str, pipeline_name: str, runtime_params: Dict[str, Any]) -> None:
        g.started.set()
        g.release.wait(5)
        g.ran.append({"execution_id": execution_id, "params": runtime_params})
        try:
            if g.error is not None:
                raise g.error
        finally:
            g.finished.set()

    monkeypatch.setattr(PipelineRunner, "run_pipeline_jobs", run_pipeline_jobs)
    return g


def _runner_factory(sessions: sessionmaker[Session]) -> Any:
    def make() -> PipelineRunner:
        db = sessions()
        return PipelineRunner(ExecutionManager(db), JobManager(db), dispatcher=None)  # type: ignore[arg-type]

    return make


def _execution(sessions: sessionmaker[Session], execution_id: str) -> Execution:
    with sessions() as db:
        row = db.query(Execution).filter(Execution.execution_id == execution_id).one()
        db.expunge(row)
        return row


def test_returns_before_the_run_finishes(sessions, gate):
    make = _runner_factory(sessions)
    make().start_pipeline(PIPE, {"day": "2026-01-01"}, "e1", background_runner=make)

    assert gate.started.wait(5)
    assert _execution(sessions, "e1").state == "pending"  # created, still running
    assert gate.ran == []
    gate.release.set()
    assert gate.finished.wait(5)
    assert gate.ran == [{"execution_id": "e1", "params": {"day": "2026-01-01"}}]


def test_an_unregistered_pipeline_raises_and_creates_nothing(sessions, gate):
    make = _runner_factory(sessions)
    with pytest.raises(ValueError, match="not found in registry"):
        make().start_pipeline("nope", {}, "e2", background_runner=make)
    with sessions() as db:
        assert db.query(Execution).count() == 0
    assert not gate.started.is_set()


def test_invalid_params_raise_and_create_nothing(sessions, gate):
    make = _runner_factory(sessions)
    with pytest.raises(ValueError, match="Invalid parameters"):
        make().start_pipeline(PIPE, {}, "e3", background_runner=make)
    with sessions() as db:
        assert db.query(Execution).count() == 0


def test_a_failure_in_the_background_marks_the_execution_failed(sessions, gate):
    make = _runner_factory(sessions)
    gate.error = RuntimeError("source unreachable")
    gate.release.set()
    make().start_pipeline(PIPE, {"day": "d"}, "e4", background_runner=make)

    assert gate.finished.wait(5)
    for _ in range(100):  # the state is written just after run_pipeline_jobs raises
        execution = _execution(sessions, "e4")
        if execution.state == "failed":
            break
        threading.Event().wait(0.02)
    assert (execution.state, execution.error_message) == ("failed", "source unreachable")


def test_a_long_dlq_run_no_longer_blocks_the_poll(sessions, gate):
    """The poll returns (and commits, releasing its locks) while the run is still going."""
    with sessions() as db:
        db.add(
            DLQJob(
                job_payload={"day": "d"},
                pipeline_name=PIPE,
                scheduled_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=1),
                delay_minutes=0,
                status="pending",
                retry_count=0,
                max_retries=3,
            )
        )
        db.commit()

    DLQScheduler(pipeline_runner_factory=_runner_factory(sessions))._poll_and_process()

    assert gate.started.wait(5)
    assert not gate.finished.is_set()  # the run is still in progress...
    with sessions() as db:
        job = db.query(DLQJob).one()
        assert job.status == "completed"  # ...yet the DLQ job is done and unlocked
        assert job.execution_id is not None
    gate.release.set()
    assert gate.finished.wait(5)
