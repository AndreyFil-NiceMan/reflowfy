"""ReflowManager HTTP routes in app.py: health, pipelines, /run, pause/resume, retry-failed,
jobs/errors/checkpoints and stats, against a real SQLite database.

Background work (planning, the dispatch loop) is replaced by recorders unless a test runs it
on purpose; `TestClient` without a `with` block never runs the app's lifespan, so nothing
connects to Postgres or Kafka.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from typing_extensions import Annotated, NotRequired, Required

import reflowfy.reflow_manager.app as app_module
from reflowfy import __version__
from reflowfy.core.abstract_pipeline import AbstractPipeline, ScheduledRun
from reflowfy.core.runtime_params import Param, RuntimeParams
from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.reflow_manager.models import Base, Execution, Job, PipelineSchedule
from reflowfy.reflow_manager.pipeline_runner import PipelineRunner
from reflowfy.reflow_manager.pipeline_scheduler import PipelineScheduler
from reflowfy.sources.static import StaticSource


class _RouteParams(RuntimeParams, total=False):
    env: Required[Literal["dev", "prod"]]
    size: Annotated[NotRequired[int], Param("How many", default=2)]


class _RoutesPipeline(AbstractPipeline[_RouteParams]):
    name = "phase1_app_routes_pipeline"

    def define_source(self, runtime_params: Any) -> Any:
        return StaticSource([{"id": 1}, {"id": 2}, {"id": 3}])

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        raise AssertionError("dispatch is stubbed out")


class _ScheduledRoutesPipeline(AbstractPipeline[RuntimeParams]):
    name = "phase1_app_routes_scheduled"
    schedules = [ScheduledRun(name="hourly", cron="0 * * * *")]

    def define_source(self, runtime_params: Any) -> Any:
        return StaticSource([{"id": 1}])

    def define_transformations(self, records: Any, runtime_params: Any) -> Any:
        return []

    def define_destination(self, records: Any, runtime_params: Any) -> Any:
        raise AssertionError("dispatch is stubbed out")


PIPE = _RoutesPipeline.name
SCHEDULED = _ScheduledRoutesPipeline.name


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/app.db",
        connect_args={"timeout": 30, "check_same_thread": False},
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def factory(engine):
    return sessionmaker(bind=engine)


@pytest.fixture
def db(factory) -> Session:
    session = factory()
    yield session
    session.close()


@pytest.fixture
def background(monkeypatch) -> Dict[str, List[Any]]:
    """Record the background tasks the routes schedule instead of running them."""
    calls: Dict[str, List[Any]] = {"dispatch": [], "resume": []}
    monkeypatch.setattr(
        app_module, "_dispatch_pipeline_jobs", lambda **kw: calls["dispatch"].append(kw)
    )
    monkeypatch.setattr(
        app_module, "_resume_execution_sync", lambda eid: calls["resume"].append(eid)
    )
    return calls


@pytest.fixture
def client(factory, monkeypatch, background):
    monkeypatch.setenv("EXECUTION_MODE", "local")
    monkeypatch.setattr(app_module, "SessionLocal", factory)
    monkeypatch.setattr(app_module, "get_pipeline_scheduler", lambda: None)

    def db_override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    def manager_override():
        session = factory()
        try:
            yield ReflowManager(session, execution_mode="local")
        finally:
            session.close()

    app_module.app.dependency_overrides[get_db] = db_override
    app_module.app.dependency_overrides[app_module.get_reflow_manager] = manager_override
    yield TestClient(app_module.app)
    app_module.app.dependency_overrides.clear()


def _execution(db: Session, state: str, eid: str = "e", **cols: Any) -> None:
    cols.setdefault("plan_complete", True)
    db.add(Execution(execution_id=eid, pipeline_name=PIPE, state=state, **cols))
    db.commit()


def _job(db: Session, job_id: str, state: str, eid: str = "e", **cols: Any) -> None:
    cols.setdefault("batch_number", 1)
    db.add(
        Job(job_id=job_id, execution_id=eid, job_payload={"job_id": job_id}, state=state, **cols)
    )
    db.commit()


def _execution_row(db: Session, eid: str = "e") -> Execution:
    db.expire_all()
    return db.query(Execution).filter(Execution.execution_id == eid).one()


# ----- health, metrics, registry ----------------------------------------------------


def test_health_is_503_until_the_database_is_ready(client, monkeypatch):
    monkeypatch.setattr(app_module, "_db_ready", False)
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {
        "status": "starting",
        "service": "reflow-manager",
        "version": __version__,
    }


def test_health_once_the_database_is_ready(client, monkeypatch):
    monkeypatch.setattr(app_module, "_db_ready", True)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy" and response.json()["version"] == __version__


def test_metrics_endpoint_serves_prometheus_text(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "reflowfy_jobs_processed_total" in response.text


def test_get_and_list_pipelines(client):
    body = client.get(f"/pipelines/{PIPE}").json()
    assert body["name"] == PIPE
    params = {p["name"]: p for p in body["parameters"]}
    assert params["env"]["required"] is True and params["size"]["default"] == 2

    names = {p["name"] for p in client.get("/pipelines").json()}
    assert {PIPE, SCHEDULED} <= names


def test_get_unknown_pipeline_is_404(client):
    response = client.get("/pipelines/phase1_no_such_pipeline")
    assert response.status_code == 404
    assert "phase1_no_such_pipeline" in response.json()["detail"]


# ----- /run -------------------------------------------------------------------------


def test_run_unknown_pipeline_is_404_and_creates_nothing(client, db, background):
    response = client.post("/run", json={"pipeline_name": "phase1_no_such_pipeline"})
    assert response.status_code == 404
    assert db.query(Execution).count() == 0
    assert background["dispatch"] == []


@pytest.mark.parametrize(
    "params, fragment",
    [({}, "env"), ({"env": "staging"}, "env"), ({"env": "dev", "size": "many"}, "size")],
)
def test_run_with_invalid_parameters_is_422(client, db, background, params, fragment):
    response = client.post("/run", json={"pipeline_name": PIPE, "runtime_params": params})

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail.startswith("Invalid parameters") and fragment in detail
    assert db.query(Execution).count() == 0
    assert background["dispatch"] == []


def test_run_accepts_persists_and_schedules_the_dispatch(client, db, background):
    response = client.post(
        "/run",
        json={
            "pipeline_name": PIPE,
            "runtime_params": {"env": "prod"},
            "rate_limit": 120,
            "execution_id": "run-1",
            "mode": "local",
            "enable_duplicate_jobs": False,
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["execution_id"] == "run-1" and body["state"] == "pending"
    assert body["status_url"] == "/executions/run-1"
    assert "warnings" not in body

    execution = _execution_row(db, "run-1")
    assert execution.state == "pending" and execution.plan_complete is False
    assert execution.rate_limit == 120  # reused by a resume after a manager restart
    assert execution.runtime_params == {"env": "prod"}

    assert background["dispatch"] == [
        {
            "execution_id": "run-1",
            "pipeline_name": PIPE,
            "runtime_params": {"env": "prod"},
            "rate_limit_override": 120,
            "mode": "local",
            "enable_duplicate_jobs": False,
        }
    ]


def test_run_generates_an_execution_id(client, db):
    body = client.post(
        "/run", json={"pipeline_name": PIPE, "runtime_params": {"env": "dev"}}
    ).json()
    assert len(body["execution_id"]) == 36  # a uuid4
    assert _execution_row(db, body["execution_id"]).rate_limit is None


def test_run_with_an_existing_execution_id_is_409(client, db, background):
    _execution(db, "completed", eid="dup")
    response = client.post(
        "/run",
        json={"pipeline_name": PIPE, "runtime_params": {"env": "dev"}, "execution_id": "dup"},
    )
    assert response.status_code == 409
    assert _execution_row(db, "dup").state == "completed"
    assert background["dispatch"] == []


def test_run_warns_about_undeclared_parameters(client):
    response = client.post(
        "/run", json={"pipeline_name": PIPE, "runtime_params": {"env": "dev", "szie": 3, "_x": 1}}
    )
    assert response.status_code == 202
    (warning,) = response.json()["warnings"]
    assert "szie" in warning and "_x" not in warning  # underscore keys are internal


def test_run_unexpected_error_is_500(client, monkeypatch):
    def broken(db, mode=None):
        raise RuntimeError("kafka config exploded")

    monkeypatch.setattr(app_module, "_make_manager", broken)
    response = client.post("/run", json={"pipeline_name": PIPE, "runtime_params": {"env": "dev"}})
    assert response.status_code == 500
    assert "kafka config exploded" in response.json()["detail"]


def test_run_resets_the_schedule_of_a_scheduled_pipeline(client, db, monkeypatch):
    scheduler = PipelineScheduler(pipeline_runner_factory=lambda: None)  # never started
    monkeypatch.setattr(app_module, "get_pipeline_scheduler", lambda: scheduler)
    long_ago = datetime(2020, 1, 1)
    db.add(
        PipelineSchedule(
            pipeline_name=SCHEDULED,
            schedule_name="hourly",
            cron_expression="0 * * * *",
            runtime_params={},
            next_run_at=long_ago,  # overdue: the scheduler would fire it right away
        )
    )
    db.commit()
    before = _now()

    assert client.post("/run", json={"pipeline_name": SCHEDULED}).status_code == 202

    db.expire_all()
    row = db.query(PipelineSchedule).one()
    assert row.last_triggered_at is not None and row.last_triggered_at >= before
    assert row.next_run_at > before  # pushed to the next hour after the manual run
    assert row.next_run_at <= before + timedelta(hours=1, minutes=1)


def test_run_does_not_touch_schedules_of_an_unscheduled_pipeline(client, monkeypatch):
    class _Recorder:
        def __init__(self) -> None:
            self.calls: List[Any] = []

        def reset_schedule(self, *args: Any, **kwargs: Any) -> None:
            self.calls.append(args)

    recorder = _Recorder()
    monkeypatch.setattr(app_module, "get_pipeline_scheduler", lambda: recorder)
    response = client.post("/run", json={"pipeline_name": PIPE, "runtime_params": {"env": "dev"}})
    assert response.status_code == 202
    assert recorder.calls == []


@pytest.mark.parametrize(
    "enable_duplicate_jobs, dedup", [(False, True), (None, False), (True, False)]
)
def test_dispatch_task_sets_dedup_check_on_the_jobs(
    factory, db, monkeypatch, enable_duplicate_jobs, dedup
):
    """The real background task plans the jobs; `enable_duplicate_jobs=false` marks each
    payload for the worker's content dedup."""
    monkeypatch.setenv("EXECUTION_MODE", "local")
    monkeypatch.setattr(app_module, "SessionLocal", factory)
    monkeypatch.setattr(PipelineRunner, "_drive_execution", lambda *a, **k: (0, 0, 0))
    db.add(Execution(execution_id="d", pipeline_name=PIPE, state="pending", plan_complete=False))
    db.commit()

    app_module._dispatch_pipeline_jobs(
        execution_id="d",
        pipeline_name=PIPE,
        runtime_params={"env": "dev"},
        enable_duplicate_jobs=enable_duplicate_jobs,
    )

    db.expire_all()
    jobs = db.query(Job).filter(Job.execution_id == "d").all()
    assert jobs, "planning saved no jobs"
    assert {j.job_payload["dedup_check"] for j in jobs} == {dedup}
    assert _execution_row(db, "d").plan_complete is True


def test_dispatch_task_failure_marks_the_execution_failed(factory, db, monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "local")
    monkeypatch.setattr(app_module, "SessionLocal", factory)

    def explode(self, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("planner crashed")

    monkeypatch.setattr(ReflowManager, "run_pipeline_jobs", explode)
    db.add(Execution(execution_id="f", pipeline_name=PIPE, state="pending", plan_complete=False))
    db.commit()

    app_module._dispatch_pipeline_jobs(execution_id="f", pipeline_name=PIPE, runtime_params={})

    execution = _execution_row(db, "f")
    assert execution.state == "failed" and execution.error_message == "planner crashed"
    assert execution.completed_at is not None


# ----- pause / resume ---------------------------------------------------------------


@pytest.mark.parametrize(
    "state, code",
    [
        ("pending", 200),
        ("running", 200),
        ("paused", 409),
        ("completed", 409),
        ("failed", 409),
        ("cancelled", 409),
    ],
)
def test_pause_from_every_state(client, db, state, code):
    _execution(db, state)

    response = client.post("/executions/e/pause")

    assert response.status_code == code
    expected = "paused" if code == 200 else state
    assert _execution_row(db).state == expected
    if code == 200:
        assert response.json()["execution"]["state"] == "paused"


def test_pause_unknown_is_404(client):
    assert client.post("/executions/nope/pause").status_code == 404


@pytest.mark.parametrize(
    "state, code",
    [
        ("paused", 200),
        ("pending", 400),
        ("running", 400),
        ("completed", 400),
        ("failed", 400),
        ("cancelled", 400),
    ],
)
def test_resume_from_every_state(client, db, background, state, code):
    _execution(db, state)

    response = client.post("/executions/e/resume")

    assert response.status_code == code
    if code == 200:
        assert _execution_row(db).state == "running"
        assert background["resume"] == ["e"]  # the dispatch loop is restarted
    else:
        assert _execution_row(db).state == state
        assert background["resume"] == []


def test_resume_unknown_is_400(client, background):
    assert client.post("/executions/nope/resume").status_code == 400
    assert background["resume"] == []


# ----- retry-failed -----------------------------------------------------------------


def _failed_run(db: Session, state: str = "failed") -> None:
    _execution(db, state, error_message="2 jobs failed", completed_at=_now())
    _job(db, "j1", "failed", error_message="timeout talking to ES", attempts=3)
    _job(db, "j2", "failed", error_message="bad record", attempts=1)
    _job(db, "j3", "completed")


def test_retry_failed_requeues_every_failed_job(client, db, background):
    _failed_run(db)

    response = client.post("/executions/e/retry-failed")

    assert response.status_code == 200
    assert response.json() == {"execution_id": "e", "requeued": 2}
    db.expire_all()
    jobs = {j.job_id: j for j in db.query(Job).all()}
    for job_id in ("j1", "j2"):
        assert jobs[job_id].state == "pending"
        assert jobs[job_id].attempts == 0 and jobs[job_id].error_message is None
    assert jobs["j3"].state == "completed"
    execution = _execution_row(db)
    assert execution.state == "running"
    assert execution.completed_at is None and execution.error_message is None
    assert background["resume"] == ["e"]


def test_retry_failed_filters_by_error(client, db, background):
    _failed_run(db)

    response = client.post("/executions/e/retry-failed", params={"error_contains": "timeout"})

    assert response.json()["requeued"] == 1
    db.expire_all()
    assert {j.job_id: j.state for j in db.query(Job).all()} == {
        "j1": "pending",
        "j2": "failed",
        "j3": "completed",
    }


def test_retry_failed_on_a_running_execution_reuses_its_loop(client, db, background):
    _failed_run(db, state="running")
    assert client.post("/executions/e/retry-failed").json()["requeued"] == 2
    assert _execution_row(db).state == "running"
    assert background["resume"] == []  # the running loop picks the jobs up


def test_retry_failed_with_nothing_to_retry_leaves_the_execution(client, db, background):
    _execution(db, "completed")
    _job(db, "ok", "completed")

    response = client.post("/executions/e/retry-failed")

    assert response.json()["requeued"] == 0
    assert _execution_row(db).state == "completed"
    assert background["resume"] == []


def test_retry_failed_errors(client, db):
    _execution(db, "paused")
    assert client.post("/executions/e/retry-failed").status_code == 409
    assert client.post("/executions/nope/retry-failed").status_code == 404


# ----- jobs, errors, checkpoints, stats ---------------------------------------------


@pytest.fixture
def with_jobs(db: Session) -> Session:
    _execution(db, "running")
    _job(db, "j1", "completed", processed_records=10)
    _job(
        db,
        "j2",
        "failed",
        error_message="boom",
        error_traceback="Traceback...",
        completed_at=datetime(2026, 1, 1, 0, 0, 5),
    )
    _job(db, "j3", "pending", batch_number=2)
    _job(db, "j4", "failed", batch_number=2, error_message="bang")
    return db


def test_jobs_list_and_state_filter(client, with_jobs):
    jobs = client.get("/executions/e/jobs").json()
    assert {j["job_id"]: j["state"] for j in jobs} == {
        "j1": "completed",
        "j2": "failed",
        "j3": "pending",
        "j4": "failed",
    }
    failed = client.get("/executions/e/jobs", params={"state": "failed"}).json()
    assert {j["job_id"] for j in failed} == {"j2", "j4"}
    assert client.get("/executions/e/jobs", params={"state": "running"}).json() == []


def test_jobs_of_an_unknown_execution_is_404(client):
    assert client.get("/executions/nope/jobs").status_code == 404


def test_jobs_of_an_execution_without_jobs_is_empty(client, db):
    _execution(db, "pending")
    assert client.get("/executions/e/jobs").json() == []


def test_errors_lists_failed_jobs(client, with_jobs):
    errors = {e["job_id"]: e for e in client.get("/executions/e/errors").json()}
    assert set(errors) == {"j2", "j4"}
    assert errors["j2"] == {
        "job_id": "j2",
        "batch_number": 1,
        "error_message": "boom",
        "error_traceback": "Traceback...",
        "failed_at": "2026-01-01T00:00:05",
    }
    assert errors["j4"]["failed_at"] is None and errors["j4"]["batch_number"] == 2


def test_errors_of_a_clean_or_unknown_execution(client, db):
    _execution(db, "completed")
    assert client.get("/executions/e/errors").json() == []
    assert client.get("/executions/nope/errors").status_code == 404


def test_checkpoints_is_deprecated_but_answers(client, with_jobs):
    response = client.get("/executions/e/checkpoints", params={"state": "pending"})
    assert response.status_code == 200
    assert response.headers["Deprecation"] == "true"
    assert "/executions/e/jobs" in response.headers["Link"]
    assert [j["job_id"] for j in response.json()] == ["j3"]


def test_execution_stats_route(client, with_jobs):
    body = client.get("/executions/e/stats").json()
    assert body["execution_id"] == "e" and body["state"] == "running"
    assert (body["jobs_completed"], body["jobs_failed"], body["jobs_pending"]) == (1, 2, 1)
    assert "jobs_cancelled" in body
    assert [c["batch_number"] for c in body["checkpoints"]] == [1, 2]


def test_execution_stats_route_errors(client, monkeypatch):
    assert client.get("/executions/nope/stats").status_code == 404

    def broken(self, execution_id):
        raise RuntimeError("db gone")

    monkeypatch.setattr(ReflowManager, "get_execution_stats", broken)
    response = client.get("/executions/e/stats")
    assert response.status_code == 500 and "db gone" in response.json()["detail"]
