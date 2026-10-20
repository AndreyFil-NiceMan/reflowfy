"""`/stats/*` routes (stats_routes.py) against a real SQLite database.

Each endpoint is checked on an empty database, on a database holding executions and jobs in
every state, and with its pipeline filter, asserting that the counts add up.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.models import Base, DLQJob, DLQJobArchive, Execution, Job

T0 = datetime(2026, 1, 1, 12, 0, 0)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/stats.db",
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


def _execution(db: Session, eid: str, pipeline: str, state: str, minutes: int, **cols: Any) -> None:
    db.add(
        Execution(
            execution_id=eid,
            pipeline_name=pipeline,
            state=state,
            created_at=T0 + timedelta(minutes=minutes),
            **cols,
        )
    )


def _jobs(db: Session, eid: str, **states: int) -> None:
    for state, count in states.items():
        for i in range(count):
            db.add(
                Job(
                    job_id=f"{eid}-{state}-{i}",
                    execution_id=eid,
                    job_payload={},
                    batch_number=1,
                    state=state,
                )
            )


def _dlq(db: Session, pipeline: str, status: str, retry_count: int = 0, minutes: int = 0) -> None:
    db.add(
        DLQJob(
            job_payload={"k": 1},
            pipeline_name=pipeline,
            scheduled_at=T0,
            delay_minutes=5,
            status=status,
            retry_count=retry_count,
            created_at=T0 + timedelta(minutes=minutes),
        )
    )


def _archive(db: Session, pipeline: str) -> None:
    db.add(
        DLQJobArchive(
            job_payload={},
            pipeline_name=pipeline,
            delay_minutes=5,
            retry_count=5,
            max_retries=5,
            created_at=T0,
        )
    )


@pytest.fixture
def mixed(db: Session) -> Session:
    """alpha: 3 executions / 11 jobs, beta: 2 executions / 3 jobs, gamma: 1 execution, no jobs."""
    _execution(db, "a1", "alpha", "completed", 1, total_jobs=4, jobs_completed=3, jobs_failed=1)
    _execution(
        db, "a2", "alpha", "failed", 2, error_message="boom", completed_at=T0 + timedelta(hours=1)
    )
    _execution(db, "a3", "alpha", "running", 3)
    _execution(db, "b1", "beta", "completed", 4)
    _execution(db, "b2", "beta", "failed", 5, completed_at=T0 + timedelta(hours=2))
    _execution(db, "g1", "gamma", "pending", 6)
    db.flush()
    _jobs(db, "a1", completed=3, failed=1)
    _jobs(db, "a2", failed=2, cancelled=1)
    _jobs(db, "a3", pending=1, running=1, deduplicated=1, dispatched=1)
    _jobs(db, "b1", completed=2)
    _jobs(db, "b2", failed=1)
    _dlq(db, "alpha", "pending", 0, minutes=10)
    _dlq(db, "alpha", "pending", 0, minutes=5)
    _dlq(db, "alpha", "failed", 3)
    _dlq(db, "beta", "completed", 1)
    _archive(db, "alpha")
    _archive(db, "gamma")
    db.commit()
    return db


def _by_name(pipelines: list) -> Dict[str, Dict[str, Any]]:
    return {p["pipeline_name"]: p for p in pipelines}


# ----- /stats/overview --------------------------------------------------------------


def test_overview_on_an_empty_database(client):
    body = client.get("/stats/overview").json()
    assert body == {
        "total_executions": 0,
        "executions_by_state": {},
        "total_jobs": 0,
        "jobs_by_state": {},
        "success_rate": 100.0,
        "dlq": {},
    }


def test_overview_counts_every_state(client, mixed):
    body = client.get("/stats/overview").json()

    assert body["total_executions"] == 6
    assert body["executions_by_state"] == {
        "completed": 2,
        "failed": 2,
        "running": 1,
        "pending": 1,
    }
    assert body["jobs_by_state"] == {
        "completed": 5,
        "failed": 4,
        "cancelled": 1,
        "pending": 1,
        "running": 1,
        "deduplicated": 1,
        "dispatched": 1,
    }
    assert body["total_jobs"] == sum(body["jobs_by_state"].values()) == 14
    # cancelled / deduplicated jobs do not count as finished for the success rate
    assert body["success_rate"] == round(5 / 9 * 100, 2)
    assert body["dlq"] == {"pending": 2, "failed": 1, "completed": 1}


# ----- /stats/pipelines -------------------------------------------------------------


def test_pipeline_stats_on_an_empty_database(client):
    assert client.get("/stats/pipelines").json() == {"pipelines": []}


def test_pipeline_stats_per_pipeline(client, mixed):
    pipelines = _by_name(client.get("/stats/pipelines").json()["pipelines"])

    assert set(pipelines) == {"alpha", "beta", "gamma"}
    alpha = pipelines["alpha"]
    assert alpha["total_executions"] == 3
    assert alpha["executions_by_state"] == {"completed": 1, "failed": 1, "running": 1}
    assert (alpha["total_jobs"], alpha["jobs_completed"], alpha["jobs_failed"]) == (11, 3, 3)
    assert alpha["success_rate"] == 50.0
    assert alpha["last_execution_at"] == (T0 + timedelta(minutes=3)).isoformat()

    beta = pipelines["beta"]
    assert (beta["total_jobs"], beta["jobs_completed"], beta["jobs_failed"]) == (3, 2, 1)
    assert beta["success_rate"] == round(2 / 3 * 100, 2)

    # an execution with no jobs yet still gets every field
    gamma = pipelines["gamma"]
    assert gamma["total_executions"] == 1
    assert (gamma["total_jobs"], gamma["jobs_completed"], gamma["jobs_failed"]) == (0, 0, 0)
    assert gamma["success_rate"] == 100.0

    assert sum(p["total_jobs"] for p in pipelines.values()) == 14


# ----- /stats/pipelines/{name} ------------------------------------------------------


def test_pipeline_detail_unknown_pipeline_is_404(client, monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    monkeypatch.setattr(pipeline_registry, "get", lambda name: None)
    response = client.get("/stats/pipelines/nope")
    assert response.status_code == 404
    assert "nope" in response.json()["detail"]


def test_pipeline_detail_registered_but_never_run(client, monkeypatch):
    from reflowfy.core.registry import pipeline_registry

    meta = {"name": "fresh", "rate_limit": 7, "parameters": [], "schedule": None}
    monkeypatch.setattr(
        pipeline_registry,
        "get",
        lambda name: SimpleNamespace(to_dict=lambda: meta) if name == "fresh" else None,
    )

    body = client.get("/stats/pipelines/fresh").json()

    assert body["pipeline_name"] == "fresh"
    assert body["total_executions"] == 0 and body["recent_executions"] == []
    assert body["success_rate"] == 100.0
    assert body["rate_limit"] == 7  # registry metadata merged in
    assert "name" not in body  # ... but not its duplicate "name" key


def test_pipeline_detail_with_history(client, mixed):
    body = client.get("/stats/pipelines/alpha").json()

    assert body["total_executions"] == 3
    assert body["executions_by_state"] == {"completed": 1, "failed": 1, "running": 1}
    assert (body["total_jobs"], body["jobs_completed"], body["jobs_failed"]) == (11, 3, 3)
    assert body["success_rate"] == 50.0
    recent = body["recent_executions"]
    assert [e["execution_id"] for e in recent] == ["a3", "a2", "a1"]  # newest first
    a2 = recent[1]
    assert a2["state"] == "failed" and a2["error_message"] == "boom"
    assert a2["completed_at"] == (T0 + timedelta(hours=1)).isoformat()
    assert recent[0]["completed_at"] is None
    a1 = recent[2]
    assert (a1["total_jobs"], a1["jobs_completed"], a1["jobs_failed"]) == (4, 3, 1)


def test_pipeline_detail_limit(client, mixed):
    body = client.get("/stats/pipelines/alpha", params={"limit": 2}).json()
    assert [e["execution_id"] for e in body["recent_executions"]] == ["a3", "a2"]
    assert body["total_executions"] == 3  # the limit only trims the list


# ----- /stats/failures --------------------------------------------------------------


def test_failures_on_an_empty_database(client):
    assert client.get("/stats/failures").json() == {
        "failed_executions": [],
        "total_failed_executions": 0,
        "total_failed_jobs": 0,
    }


def test_failures_across_pipelines(client, mixed):
    body = client.get("/stats/failures").json()

    assert body["total_failed_executions"] == 2
    assert body["total_failed_jobs"] == 4  # alpha 3 + beta 1, any execution state
    # most recently completed first
    assert [e["execution_id"] for e in body["failed_executions"]] == ["b2", "a2"]
    a2 = body["failed_executions"][1]
    assert a2["pipeline_name"] == "alpha" and a2["error_message"] == "boom"


def test_failures_filtered_by_pipeline_and_limited(client, mixed):
    alpha = client.get("/stats/failures", params={"pipeline_name": "alpha"}).json()
    assert alpha["total_failed_executions"] == 1
    assert alpha["total_failed_jobs"] == 3
    assert [e["execution_id"] for e in alpha["failed_executions"]] == ["a2"]

    limited = client.get("/stats/failures", params={"limit": 1}).json()
    assert len(limited["failed_executions"]) == 1
    assert limited["total_failed_executions"] == 2  # the total ignores the limit

    none = client.get("/stats/failures", params={"pipeline_name": "gamma"}).json()
    assert none["total_failed_executions"] == 0 and none["total_failed_jobs"] == 0


# ----- /stats/dlq -------------------------------------------------------------------


def test_dlq_stats_on_an_empty_database(client):
    assert client.get("/stats/dlq").json() == {
        "total_dlq_jobs": 0,
        "jobs_by_status": {},
        "total_archived": 0,
        "pipelines": [],
    }


def test_dlq_stats_across_pipelines(client, mixed):
    body = client.get("/stats/dlq").json()

    assert body["total_dlq_jobs"] == 4
    assert body["jobs_by_status"] == {"pending": 2, "failed": 1, "completed": 1}
    assert body["total_archived"] == 2
    pipelines = _by_name(body["pipelines"])
    assert set(pipelines) == {"alpha", "beta", "gamma"}

    alpha = pipelines["alpha"]
    assert (alpha["pending"], alpha["failed"], alpha["completed"]) == (2, 1, 0)
    assert alpha["archived"] == 1
    assert alpha["oldest_pending_at"] == (T0 + timedelta(minutes=5)).isoformat()

    assert pipelines["beta"]["completed"] == 1 and pipelines["beta"]["oldest_pending_at"] is None
    assert pipelines["beta"]["avg_retry_count"] == 1.0
    # a pipeline with only archived jobs still appears
    gamma = pipelines["gamma"]
    assert gamma["archived"] == 1 and gamma["pending"] == 0 and gamma["avg_retry_count"] == 0.0


def test_dlq_stats_filtered_by_pipeline(client, mixed):
    body = client.get("/stats/dlq", params={"pipeline_name": "alpha"}).json()

    assert body["total_dlq_jobs"] == 3
    assert body["jobs_by_status"] == {"pending": 2, "failed": 1}
    assert body["total_archived"] == 1
    assert [p["pipeline_name"] for p in body["pipelines"]] == ["alpha"]

    gamma = client.get("/stats/dlq", params={"pipeline_name": "gamma"}).json()
    assert gamma["total_dlq_jobs"] == 0 and gamma["total_archived"] == 1
    assert [p["pipeline_name"] for p in gamma["pipelines"]] == ["gamma"]


def test_dlq_avg_retry_count_covers_every_status(client, mixed):
    alpha = _by_name(client.get("/stats/dlq").json()["pipelines"])["alpha"]
    # alpha's DLQ jobs have retry counts 0, 0 and 3
    assert alpha["avg_retry_count"] == 1.0
