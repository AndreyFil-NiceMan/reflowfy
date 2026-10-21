"""`/dlq/*` routes (dlq_routes.py) against a real SQLite database.

The DLQ is a "run this pipeline later with these params" queue: scheduling stores a row,
dispatching hands its payload to `PipelineRunner.run_pipeline` (faked here) as runtime params.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
import reflowfy.reflow_manager.dlq_routes as dlq_routes
from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.dlq_scheduler import (
    DLQ_DEFAULT_DELAY_MINUTES,
    DLQ_MAX_RETRIES,
    DLQScheduler,
)
from reflowfy.reflow_manager.models import Base, DLQJob


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(
        f"sqlite:///{tmp_path}/dlq.db",
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


@pytest.fixture(autouse=True)
def _registered_pipelines():
    """POST /dlq/schedule only accepts registered pipelines; register the names used here."""
    from reflowfy.core.registry import pipeline_registry

    names = ["p", "q", "a", "b", "other"]
    added = [n for n in names if pipeline_registry.get(n) is None]
    for name in added:
        pipeline_registry._pipelines[name] = object()
    yield
    for name in added:
        pipeline_registry._pipelines.pop(name, None)


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


class _Runner:
    """Stands in for PipelineRunner: records each run_pipeline call."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.fail = fail

    def run_pipeline(self, pipeline_name: str, runtime_params: Dict[str, Any], execution_id: str):
        self.calls.append(
            {"pipeline_name": pipeline_name, "params": runtime_params, "execution_id": execution_id}
        )
        if self.fail:
            raise RuntimeError("source unreachable")
        return {}


@pytest.fixture
def runner(monkeypatch) -> _Runner:
    r = _Runner()
    scheduler = DLQScheduler(pipeline_runner_factory=lambda: r)  # never started: no thread
    monkeypatch.setattr(dlq_routes, "get_dlq_scheduler", lambda: scheduler)
    return r


def _schedule(client, pipeline: str = "p", **body: Any) -> Dict[str, Any]:
    response = client.post(
        "/dlq/schedule", json={"pipeline_name": pipeline, "job_payload": {"x": 1}, **body}
    )
    assert response.status_code == 201, response.text
    return response.json()


def _row(db: Session, job_id: int) -> DLQJob:
    db.expire_all()
    return db.query(DLQJob).filter(DLQJob.id == job_id).one()


# ----- scheduling -------------------------------------------------------------------


def test_schedule_stores_a_pending_job(client, db):
    before = _now()
    body = _schedule(client, delay_minutes=30, job_payload={"customer": 42})

    assert body["status"] == "pending"
    assert body["pipeline_name"] == "p"
    assert body["job_payload"] == {"customer": 42}
    assert body["delay_minutes"] == 30
    assert body["retry_count"] == 0 and body["max_retries"] == DLQ_MAX_RETRIES
    scheduled = datetime.fromisoformat(body["scheduled_at"])
    assert before + timedelta(minutes=30) <= scheduled <= _now() + timedelta(minutes=30)

    row = _row(db, body["id"])
    assert row.status == "pending" and row.job_payload == {"customer": 42}


def test_schedule_uses_the_default_delay(client):
    before = _now()
    body = _schedule(client)
    assert body["delay_minutes"] == DLQ_DEFAULT_DELAY_MINUTES
    scheduled = datetime.fromisoformat(body["scheduled_at"])
    assert scheduled >= before + timedelta(minutes=DLQ_DEFAULT_DELAY_MINUTES)


def test_schedule_with_a_negative_delay_is_due_at_once(client):
    """A delay in the past is accepted and the job is simply due immediately."""
    body = _schedule(client, delay_minutes=-10)
    assert datetime.fromisoformat(body["scheduled_at"]) < _now()


@pytest.mark.parametrize(
    "payload",
    [
        {"job_payload": {"x": 1}},  # no pipeline_name
        {"pipeline_name": "p"},  # no job_payload
        {"pipeline_name": "p", "job_payload": [1, 2]},  # payload must be an object
        {"pipeline_name": "p", "job_payload": {}, "delay_minutes": "soon"},
    ],
)
def test_schedule_rejects_an_invalid_request(client, db, payload):
    assert client.post("/dlq/schedule", json=payload).status_code == 422
    assert db.query(DLQJob).count() == 0


def test_schedule_rejects_an_unknown_pipeline(client):
    response = client.post(
        "/dlq/schedule",
        json={"pipeline_name": "no_such_pipeline_anywhere", "job_payload": {}},
    )
    assert response.status_code == 404


# ----- listing and reading ----------------------------------------------------------


def _seed(db: Session) -> None:
    base = _now()
    rows = [
        ("a", "pending"),
        ("a", "pending"),
        ("a", "completed"),
        ("b", "pending"),
        ("b", "failed"),
    ]
    for i, (pipeline, status) in enumerate(rows):
        db.add(
            DLQJob(
                job_payload={"i": i},
                pipeline_name=pipeline,
                scheduled_at=base,
                delay_minutes=1,
                status=status,
                created_at=base + timedelta(seconds=i),
            )
        )
    db.commit()


def test_list_all_newest_first(client, db):
    _seed(db)
    body = client.get("/dlq/jobs").json()
    assert body["total"] == 5
    assert [j["job_payload"]["i"] for j in body["jobs"]] == [4, 3, 2, 1, 0]


def test_list_filters(client, db):
    _seed(db)
    a = client.get("/dlq/jobs", params={"pipeline_name": "a"}).json()
    assert a["total"] == 3 and {j["pipeline_name"] for j in a["jobs"]} == {"a"}

    pending = client.get("/dlq/jobs", params={"status_filter": "pending"}).json()
    assert pending["total"] == 3 and {j["status"] for j in pending["jobs"]} == {"pending"}

    both = client.get("/dlq/jobs", params={"pipeline_name": "a", "status_filter": "pending"}).json()
    assert both["total"] == 2


def test_list_pagination(client, db):
    _seed(db)
    page = client.get("/dlq/jobs", params={"limit": 2, "offset": 1}).json()
    assert page["total"] == 5  # the total ignores the page
    assert [j["job_payload"]["i"] for j in page["jobs"]] == [3, 2]

    past_end = client.get("/dlq/jobs", params={"offset": 10}).json()
    assert past_end == {"jobs": [], "total": 5}


def test_get_one(client):
    created = _schedule(client, pipeline="q")
    body = client.get(f"/dlq/jobs/{created['id']}").json()
    assert body["id"] == created["id"] and body["pipeline_name"] == "q"


def test_get_unknown_is_404(client):
    response = client.get("/dlq/jobs/999")
    assert response.status_code == 404
    assert "999" in response.json()["detail"]


# ----- cancelling -------------------------------------------------------------------


def test_cancel_a_pending_job_then_again(client, db):
    job_id = _schedule(client)["id"]

    first = client.delete(f"/dlq/jobs/{job_id}")
    assert first.status_code == 200
    assert _row(db, job_id).status == "cancelled"

    again = client.delete(f"/dlq/jobs/{job_id}")
    assert again.status_code == 400  # not pending any more
    assert "cancelled" in again.json()["detail"]


def test_cancel_unknown_is_404(client):
    assert client.delete("/dlq/jobs/999").status_code == 404


# ----- dispatching ------------------------------------------------------------------


def test_dispatch_needs_the_scheduler(client, monkeypatch):
    monkeypatch.setattr(dlq_routes, "get_dlq_scheduler", lambda: None)
    assert client.post("/dlq/jobs/1/dispatch").status_code == 503
    assert client.post("/dlq/pipelines/p/dispatch").status_code == 503


def test_dispatch_one_runs_the_pipeline_with_its_payload(client, db, runner):
    job_id = _schedule(client, pipeline="p", job_payload={"customer": 7})["id"]

    response = client.post(f"/dlq/jobs/{job_id}/dispatch")

    assert response.status_code == 200
    body = response.json()
    assert body["dispatched_count"] == 1 and body["execution_id"].startswith("dlq-")
    assert len(runner.calls) == 1
    call = runner.calls[0]
    assert call["pipeline_name"] == "p" and call["execution_id"] == body["execution_id"]
    assert call["params"]["customer"] == 7  # the payload IS the runtime params
    assert call["params"]["_dlq_source"] is True and call["params"]["_dlq_job_ids"] == [job_id]

    row = _row(db, job_id)
    assert row.status == "completed" and row.execution_id == body["execution_id"]
    assert row.processed_at is not None


def test_dispatch_a_job_that_is_not_pending_is_400(client, runner):
    job_id = _schedule(client)["id"]
    client.delete(f"/dlq/jobs/{job_id}")

    response = client.post(f"/dlq/jobs/{job_id}/dispatch")

    assert response.status_code == 400
    assert "not pending" in response.json()["detail"]
    assert runner.calls == []


def test_dispatch_failure_is_500_and_schedules_a_retry(client, db, runner):
    runner.fail = True
    job_id = _schedule(client)["id"]

    response = client.post(f"/dlq/jobs/{job_id}/dispatch")

    assert response.status_code == 500
    assert "source unreachable" in response.json()["detail"]
    row = _row(db, job_id)
    assert row.retry_count == 1 and row.error_message == "source unreachable"
    assert row.status == "pending"  # rescheduled with backoff, not lost


def test_dispatch_unknown_job_is_404(client, runner):
    assert client.post("/dlq/jobs/999/dispatch").status_code == 404


def test_dispatch_unknown_job_is_not_a_success(client, runner):
    """What the bug above does today: an error, and nothing ran."""
    response = client.post("/dlq/jobs/999/dispatch")
    assert response.status_code >= 400
    assert "not found" in response.json()["detail"]
    assert runner.calls == []


def test_dispatch_all_pending_for_a_pipeline(client, db, runner):
    ids = [_schedule(client, pipeline="p", job_payload={"n": n})["id"] for n in range(3)]
    other = _schedule(client, pipeline="other")["id"]
    client.delete(f"/dlq/jobs/{ids[2]}")  # cancelled: not dispatched

    response = client.post("/dlq/pipelines/p/dispatch")

    assert response.status_code == 200
    body = response.json()
    assert body["dispatched_count"] == 2 and body["execution_id"].startswith("dlq-")
    # Different params: one execution each, each run with its own params.
    assert len(runner.calls) == 2 and body["execution_ids"][0] == body["execution_id"]
    assert sorted(c["params"]["n"] for c in runner.calls) == [0, 1]
    assert sorted(i for c in runner.calls for i in c["params"]["_dlq_job_ids"]) == sorted(ids[:2])
    assert [_row(db, i).status for i in ids] == ["completed", "completed", "cancelled"]
    assert _row(db, other).status == "pending"


def test_dispatch_all_with_nothing_pending(client, runner):
    response = client.post("/dlq/pipelines/empty/dispatch")
    assert response.status_code == 200
    assert response.json()["dispatched_count"] == 0
    assert response.json()["execution_id"] is None
    assert runner.calls == []


def test_dispatch_all_failure_is_500_and_retries_each_job(client, db, runner):
    runner.fail = True
    ids = [_schedule(client, pipeline="p")["id"] for _ in range(2)]

    response = client.post("/dlq/pipelines/p/dispatch")

    assert response.status_code == 500
    assert [_row(db, i).retry_count for i in ids] == [1, 1]
