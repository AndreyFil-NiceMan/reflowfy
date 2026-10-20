"""`/run` losing a race on the execution id, and the background dispatch task when
even marking the execution failed does not work."""

import logging
from typing import Any, Dict, List

from sqlalchemy.exc import IntegrityError

import reflowfy.reflow_manager.app as app_module
from reflowfy.reflow_manager.execution import ExecutionManager
from reflowfy.reflow_manager.manager import ReflowManager
from test_manager_app_routes import (  # type: ignore[import-not-found]  # noqa: F401
    PIPE,
    background,
    client,
    db,
    engine,
    factory,
)


def test_two_runs_racing_on_one_execution_id_answer_409(
    client, db, background, monkeypatch  # noqa: F811
):
    """Both requests passed the existence check; the second insert hits the primary key."""

    def lost_the_race(self: Any, **kwargs: Any) -> None:
        raise IntegrityError("INSERT INTO executions", {}, Exception("duplicate key"))

    monkeypatch.setattr(ReflowManager, "create_execution", lost_the_race)

    response = client.post(
        "/run",
        json={"pipeline_name": PIPE, "runtime_params": {"env": "dev"}, "execution_id": "dup"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Execution 'dup' already exists"
    assert background["dispatch"] == []


def test_dispatch_failure_is_logged_even_when_the_execution_cannot_be_marked(
    factory, monkeypatch  # noqa: F811
):
    monkeypatch.setenv("EXECUTION_MODE", "local")
    closed: List[bool] = []
    real_factory = factory

    def tracking_session() -> Any:
        session = real_factory()
        original_close = session.close

        def close() -> None:
            closed.append(True)
            original_close()

        session.close = close  # type: ignore[method-assign]
        return session

    monkeypatch.setattr(app_module, "SessionLocal", tracking_session)

    def planner_crashed(self: Any, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("planner crashed")

    def database_down(self: Any, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("database down")

    monkeypatch.setattr(ReflowManager, "run_pipeline_jobs", planner_crashed)
    monkeypatch.setattr(ExecutionManager, "update_execution_state", database_down)
    messages: List[str] = []

    class _Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            messages.append(record.getMessage())

    handler = _Handler(logging.ERROR)
    app_module.logger.addHandler(handler)
    try:
        # must not raise out of the background task
        app_module._dispatch_pipeline_jobs(execution_id="x", pipeline_name=PIPE, runtime_params={})
    finally:
        app_module.logger.removeHandler(handler)

    assert "Background job dispatch failed for x" in messages
    assert "Failed to mark execution x as failed" in messages
    assert closed == [True]  # the session is released either way
