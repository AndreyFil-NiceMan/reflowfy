"""`/schedules/*` and `/archive/jobs*` routes (schedule_routes.py) against SQLite."""

from datetime import datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.models import Base, DLQJobArchive, Execution, PipelineSchedule

T0 = datetime(2026, 3, 1, 8, 0, 0)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/sched.db",
        connect_args={"timeout": 30, "check_same_thread": False},
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


@pytest.fixture
def client(engine):
    factory = sessionmaker(bind=engine)

    def override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app_module.app.dependency_overrides[get_db] = override
    yield TestClient(app_module.app)
    app_module.app.dependency_overrides.clear()


def _schedule(db: Session, pipeline: str, name: str, **cols: Any) -> None:
    db.add(
        PipelineSchedule(
            pipeline_name=pipeline,
            schedule_name=name,
            cron_expression=cols.pop("cron", "*/5 * * * *"),
            runtime_params=cols.pop("runtime_params", {}),
            next_run_at=cols.pop("next_run_at", T0 + timedelta(minutes=5)),
            **cols,
        )
    )


def _execution(db: Session, eid: str, pipeline: str, state: str, **cols: Any) -> None:
    db.add(Execution(execution_id=eid, pipeline_name=pipeline, state=state, **cols))


@pytest.fixture
def scheduled(db: Session) -> Session:
    """`nightly` has two named schedules; one of them has run."""
    _schedule(
        db,
        "nightly",
        "hourly",
        cron="0 * * * *",
        runtime_params={"region": "eu"},
        last_execution_id="run-2",
        last_triggered_at=T0,
    )
    _schedule(db, "nightly", "daily", cron="0 3 * * *", enabled="false")
    _schedule(db, "other", "default", last_execution_id="gone")  # its execution was purged
    _execution(db, "run-1", "nightly", "completed", jobs_completed=4)
    _execution(db, "run-2", "nightly", "failed", jobs_completed=1, jobs_failed=2)
    _execution(db, "run-3", "nightly", "running")
    _execution(db, "run-4", "nightly", "pending")
    _execution(db, "run-5", "nightly", "completed")
    _execution(db, "x", "unrelated", "completed")
    db.commit()
    return db


# ----- /schedules -------------------------------------------------------------------


def test_list_schedules(client, scheduled):
    body = client.get("/schedules").json()
    assert body["total"] == 3
    # ordered by pipeline name
    assert [s["pipeline_name"] for s in body["schedules"]] == ["nightly", "nightly", "other"]


def test_list_schedules_empty(client):
    assert client.get("/schedules").json() == {"schedules": [], "total": 0}


def test_get_schedule_for_a_scheduled_pipeline(client, scheduled):
    body = client.get("/schedules/nightly").json()

    assert body["pipeline_name"] == "nightly"
    schedules = body["schedules"]
    assert [s["schedule_name"] for s in schedules] == ["daily", "hourly"]  # by name
    daily, hourly = schedules

    assert daily["enabled"] is False and daily["cron_expression"] == "0 3 * * *"
    assert "last_execution_state" not in daily  # never ran: no enrichment

    assert hourly["enabled"] is True
    assert hourly["runtime_params"] == {"region": "eu"}
    assert hourly["last_triggered_at"] == T0.isoformat()
    assert hourly["last_execution_state"] == "failed"
    assert hourly["last_execution_jobs_completed"] == 1
    assert hourly["last_execution_jobs_failed"] == 2


def test_get_schedule_whose_last_execution_is_gone(client, scheduled):
    (schedule,) = client.get("/schedules/other").json()["schedules"]
    assert schedule["last_execution_id"] == "gone"
    assert "last_execution_state" not in schedule


def test_get_schedule_for_an_unscheduled_pipeline_is_404(client, scheduled):
    response = client.get("/schedules/unrelated")  # has executions, but no schedule
    assert response.status_code == 404
    assert "unrelated" in response.json()["detail"]


# ----- /schedules/{name}/stats ------------------------------------------------------


def test_schedule_stats_aggregates_the_pipelines_executions(client, scheduled):
    body = client.get("/schedules/nightly/stats").json()

    assert body["pipeline_name"] == "nightly"
    assert body["total_executions"] == 5  # pipeline-wide; "unrelated" not counted
    assert body["completed_executions"] == 2
    assert body["failed_executions"] == 1
    assert body["running_executions"] == 1
    assert body["pending_executions"] == 1

    by_name = {s["schedule_name"]: s for s in body["schedules"]}
    assert by_name["hourly"] == {
        "schedule_name": "hourly",
        "cron_expression": "0 * * * *",
        "next_run_at": (T0 + timedelta(minutes=5)).isoformat(),
        "enabled": True,
        "last_execution_id": "run-2",
        "last_execution_state": "failed",
        "last_triggered_at": T0.isoformat(),
    }
    assert by_name["daily"]["enabled"] is False
    assert by_name["daily"]["last_execution_state"] is None
    assert by_name["daily"]["last_triggered_at"] is None


def test_schedule_stats_with_no_executions_and_a_purged_last_run(client, scheduled):
    body = client.get("/schedules/other/stats").json()
    assert body["total_executions"] == 0
    assert body["completed_executions"] == body["failed_executions"] == 0
    (schedule,) = body["schedules"]
    assert schedule["last_execution_id"] == "gone"
    assert schedule["last_execution_state"] is None


def test_schedule_stats_for_an_unscheduled_pipeline_is_404(client, scheduled):
    assert client.get("/schedules/unrelated/stats").status_code == 404


# ----- /archive/jobs ----------------------------------------------------------------


def _archive(db: Session, n: int, pipeline: str) -> None:
    for i in range(n):
        db.add(
            DLQJobArchive(
                job_payload={"i": i, "p": pipeline},
                pipeline_name=pipeline,
                delay_minutes=5,
                retry_count=5,
                max_retries=5,
                error_message=f"boom {i}",
                created_at=T0,
                archived_at=T0 + timedelta(minutes=i if pipeline == "a" else 100 + i),
            )
        )
    db.commit()


def test_archived_jobs_list_newest_first_with_pagination(client, db):
    _archive(db, 3, "a")
    _archive(db, 2, "b")

    body = client.get("/archive/jobs").json()
    assert body["total"] == 5 and body["limit"] == 100 and body["offset"] == 0
    assert [(j["pipeline_name"], j["job_payload"]["i"]) for j in body["jobs"]] == [
        ("b", 1),
        ("b", 0),
        ("a", 2),
        ("a", 1),
        ("a", 0),
    ]

    page = client.get("/archive/jobs", params={"limit": 2, "offset": 2}).json()
    assert page["total"] == 5 and (page["limit"], page["offset"]) == (2, 2)
    assert [(j["pipeline_name"], j["job_payload"]["i"]) for j in page["jobs"]] == [
        ("a", 2),
        ("a", 1),
    ]


def test_archived_jobs_filter_and_limit_cap(client, db):
    _archive(db, 3, "a")
    _archive(db, 2, "b")

    a = client.get("/archive/jobs", params={"pipeline_name": "a"}).json()
    assert a["total"] == 3 and {j["pipeline_name"] for j in a["jobs"]} == {"a"}

    capped = client.get("/archive/jobs", params={"limit": 10_000}).json()
    assert capped["limit"] == 500  # the page size is capped


def test_archived_jobs_empty(client):
    assert client.get("/archive/jobs").json() == {"jobs": [], "total": 0, "limit": 100, "offset": 0}


def test_archived_job_detail(client, db):
    _archive(db, 1, "a")
    job_id = client.get("/archive/jobs").json()["jobs"][0]["id"]

    body = client.get(f"/archive/jobs/{job_id}").json()

    assert body["id"] == job_id
    assert body["pipeline_name"] == "a" and body["error_message"] == "boom 0"
    assert body["retry_count"] == body["max_retries"] == 5
    assert body["archived_at"] == T0.isoformat()


def test_archived_job_detail_unknown_is_404(client):
    response = client.get("/archive/jobs/12345")
    assert response.status_code == 404
    assert "12345" in response.json()["detail"]
