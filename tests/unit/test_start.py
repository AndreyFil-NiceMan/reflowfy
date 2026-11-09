"""reflow_manager/start.py: the one check before an execution is created, and the one
background driver that marks a crashed run failed."""

import logging
from typing import Any, Iterator, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from reflowfy import AbstractPipeline, RuntimeParams, pipeline_registry
from reflowfy.core.abstract_pipeline import PipelineParameter
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.start import (
    InvalidParametersError,
    PipelineNotFoundError,
    check_runnable,
    drive_execution,
)
from reflowfy.store.models import Base, Execution

PIPE = "start_module_test"


class _Pipeline(AbstractPipeline[RuntimeParams]):
    def define_parameters(self) -> List[Any]:
        return [PipelineParameter(name="day", param_type=str, required=True)]

    def define_source(self, runtime_params: Any) -> Any:
        return []

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        return None


@pytest.fixture(autouse=True)
def _propagate_logs(monkeypatch) -> None:
    """configure_logging() (run by other tests) stops "reflowfy" propagating; caplog
    listens on the root logger."""
    monkeypatch.setattr(logging.getLogger("reflowfy"), "propagate", True)


@pytest.fixture(autouse=True)
def registered() -> Iterator[None]:
    type("_Registered", (_Pipeline,), {"name": PIPE})
    yield
    pipeline_registry.unregister(PIPE)


@pytest.fixture
def sessions() -> Iterator["sessionmaker[Session]"]:
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


def test_check_runnable_returns_the_registered_pipeline() -> None:
    assert check_runnable(PIPE, {"day": "d"}).name == PIPE


def test_an_unknown_pipeline_is_a_value_error_of_its_own_type() -> None:
    with pytest.raises(PipelineNotFoundError, match="not found in registry"):
        check_runnable("nope", {})
    assert issubclass(PipelineNotFoundError, ValueError)


def test_invalid_params_are_a_value_error_of_their_own_type() -> None:
    with pytest.raises(InvalidParametersError, match="Invalid parameters"):
        check_runnable(PIPE, {})
    assert issubclass(InvalidParametersError, ValueError)


def test_drive_execution_runs_the_plan_with_the_given_arguments(sessions) -> None:
    seen: List[Any] = []

    class _Runner:
        def run_pipeline_jobs(self, **kwargs: Any) -> None:
            seen.append(kwargs)

    drive_execution(
        sessions(), lambda db: _Runner(), "e", PIPE, {"day": "d"}, rate_limit_override=5
    )

    assert seen == [
        {
            "execution_id": "e",
            "pipeline_name": PIPE,
            "runtime_params": {"day": "d"},
            "rate_limit_override": 5,
        }
    ]


def test_a_runner_that_cannot_be_built_still_fails_the_execution(sessions) -> None:
    with sessions() as db:
        ExecutionManager(db).create_execution("e", PIPE, {})

    def no_runner(db: Session) -> Any:
        raise RuntimeError("no kafka config")

    drive_execution(sessions(), no_runner, "e", PIPE, {})

    with sessions() as db:
        row = db.query(Execution).one()
        assert (row.state, row.error_message) == ("failed", "no kafka config")


def test_a_session_that_cannot_close_is_logged_not_raised(caplog) -> None:
    class _Db:
        def rollback(self) -> None:
            pass

        def close(self) -> None:
            raise RuntimeError("connection gone")

    class _Runner:
        def run_pipeline_jobs(self, **kwargs: Any) -> None:
            pass

    with caplog.at_level(logging.WARNING):
        drive_execution(_Db(), lambda db: _Runner(), "e", PIPE, {})  # type: ignore[arg-type]

    assert "Closing the database session failed" in caplog.text


def test_the_scheduler_refuses_a_pipeline_that_is_not_registered(sessions, caplog) -> None:
    """It used to create the execution and let it fail in the background."""
    from datetime import datetime, timedelta, timezone

    from reflowfy.store.models import PipelineSchedule
    import reflowfy.reflow_manager.pipeline_scheduler as ps_mod

    db = sessions()
    db.add(
        PipelineSchedule(
            pipeline_name="gone",
            schedule_name="default",
            cron_expression="*/5 * * * *",
            runtime_params={},
            next_run_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=1),
            enabled="true",
        )
    )
    db.commit()
    scheduler = ps_mod.PipelineScheduler(pipeline_runner_factory=lambda: pytest.fail("not built"))
    scheduler._trigger_pipeline(
        db, db.query(PipelineSchedule).one(), datetime.now(timezone.utc).replace(tzinfo=None)
    )

    assert db.query(Execution).count() == 0
    assert "Failed to trigger scheduled pipeline 'gone'" in caplog.text
