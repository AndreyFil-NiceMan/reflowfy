"""ReflowManager service wiring the other suites skip: the lifespan hook, the
`main()` entry point, distributed-mode construction, the content-dedup sweeper
failing at startup, and two small model/query helpers."""

from datetime import datetime
from typing import Any, Dict, List

import pytest
from sqlalchemy.orm import Session

import reflowfy.reflow_manager.app as app_module
from reflowfy.reflow_manager.dispatcher import KafkaDispatcher
from reflowfy.reflow_manager.job_manager import JobManager
from reflowfy.reflow_manager.local_dispatcher import LocalDispatcher
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.reflow_manager.models import Execution, Job, RateLimitState
from test_manager_startup import (  # type: ignore[import-not-found]  # noqa: F401
    db,
    engine,
    factory,
    startup_spies,
)


async def test_lifespan_starts_up_before_serving_and_shuts_down_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: List[str] = []

    async def startup() -> None:
        events.append("startup")

    async def shutdown() -> None:
        events.append("shutdown")

    monkeypatch.setattr(app_module, "_startup", startup)
    monkeypatch.setattr(app_module, "_shutdown", shutdown)

    async with app_module.lifespan(app_module.app):
        events.append("serving")

    assert events == ["startup", "serving", "shutdown"]


async def test_startup_survives_the_dedup_sweeper_failing_to_start(
    startup_spies: Dict[str, Any], monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    def boom() -> None:
        raise RuntimeError("no thread for the sweeper")

    monkeypatch.setattr(app_module, "init_content_dedup_scheduler", boom)

    await app_module._startup()

    assert startup_spies["order"][-1] == "pipeline_scheduler"  # everything before it ran
    assert app_module._db_ready is True


def test_main_serves_the_app_on_the_configured_address(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    served: List[Any] = []
    monkeypatch.setattr(uvicorn, "run", lambda app, host, port: served.append((app, host, port)))
    monkeypatch.setenv("HOST", "127.0.0.1")
    monkeypatch.setenv("PORT", "9101")

    app_module.main()

    assert served == [(app_module.app, "127.0.0.1", 9101)]


def test_a_mode_override_selects_the_dispatcher(
    db: Session, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    monkeypatch.setenv("EXECUTION_MODE", "local")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092")

    default = app_module._make_manager(db)
    distributed = app_module._make_manager(db, mode="distributed")

    assert isinstance(default.dispatcher, LocalDispatcher)
    assert isinstance(distributed.dispatcher, KafkaDispatcher)
    assert distributed.execution_mode == "distributed"
    assert distributed.kafka_bootstrap_servers == "kafka-1:9092"


def test_the_request_dependency_builds_a_manager_on_the_request_session(
    db: Session, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    monkeypatch.setenv("EXECUTION_MODE", "local")
    manager = app_module.get_reflow_manager(db)
    assert isinstance(manager, ReflowManager)
    assert manager.job_manager.db is db


def test_run_pipeline_is_delegated_to_the_runner(
    db: Session, monkeypatch: pytest.MonkeyPatch  # noqa: F811
) -> None:
    manager = ReflowManager(db, execution_mode="local")
    calls: List[Any] = []
    monkeypatch.setattr(
        manager.pipeline_runner, "run_pipeline", lambda *a: calls.append(a) or {"ok": True}
    )

    assert manager.run_pipeline("p", {"x": 1}, "e", 30.0) == {"ok": True}
    assert calls == [("p", {"x": 1}, "e", 30.0)]


def test_rate_limit_state_serialises() -> None:
    state = RateLimitState(
        pipeline_name="p",
        tokens=2.5,
        max_tokens=10.0,
        refill_rate=0.5,
        last_update=datetime(2026, 1, 2, 3, 4, 5),
    )
    assert state.to_dict() == {
        "pipeline_name": "p",
        "tokens": 2.5,
        "max_tokens": 10.0,
        "refill_rate": 0.5,
        "last_update": "2026-01-02T03:04:05",
    }


def test_job_counts_include_rows_in_an_unknown_state_in_the_total(
    db: Session,  # noqa: F811
) -> None:
    """A state the counter does not know (e.g. written by a newer version) is still a job."""
    db.add(Execution(execution_id="e", pipeline_name="p", state="running"))
    for job_id, state in [("a", "completed"), ("b", "completed"), ("c", "quarantined")]:
        db.add(Job(job_id=job_id, execution_id="e", job_payload={}, state=state))
    db.commit()

    counts = JobManager(db).get_job_counts("e")

    assert counts["total"] == 3
    assert counts["completed"] == 2
    assert "quarantined" not in counts
