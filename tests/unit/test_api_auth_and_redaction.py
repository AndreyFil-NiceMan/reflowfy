"""REFLOWFY_API_TOKEN bearer auth on the API and manager, and redacted job payloads in
every manager route that returns job, execution, schedule or DLQ rows."""

import logging
from datetime import datetime
from typing import Any, Iterator

import pytest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.app as app_module
from reflowfy.core.api_auth import API_TOKEN_ENV, api_token, auth_headers, install_api_auth
from reflowfy.core.secrets import REDACTED
from reflowfy.execution.distributed_executor import DistributedExecutor
from reflowfy.reflow_manager.database import get_db
from reflowfy.reflow_manager.manager import ReflowManager
from reflowfy.store.models import (
    Base,
    DLQJob,
    DLQJobArchive,
    Execution,
    Job,
    PipelineSchedule,
)

TOKEN = "s3cret-token"


@pytest.fixture(autouse=True)
def capture_reflowfy_logs(caplog: pytest.LogCaptureFixture) -> Iterator[None]:
    """setup_logging() in other tests stops "reflowfy" propagating to caplog's root handler."""
    logger = logging.getLogger("reflowfy")
    attach = not logger.propagate
    if attach:
        logger.addHandler(caplog.handler)
    yield
    if attach:
        logger.removeHandler(caplog.handler)


def _app() -> FastAPI:
    app = FastAPI()
    install_api_auth(app, service="test")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"ok": "yes"}

    @app.get("/metrics")
    def metrics() -> str:
        return "m"

    @app.get("/thing")
    def thing() -> dict[str, int]:
        return {"n": 1}

    return app


class TestMiddleware:
    def test_off_without_a_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv(API_TOKEN_ENV, raising=False)
        assert api_token() is None and auth_headers() == {}
        assert TestClient(_app()).get("/thing").status_code == 200

    def test_blank_token_is_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, "  ")
        assert api_token() is None
        assert TestClient(_app()).get("/thing").status_code == 200

    def test_requires_the_bearer_token(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
        client = TestClient(_app())
        with caplog.at_level(logging.INFO, logger="reflowfy.core.api_auth"):
            missing = client.get("/thing")
            wrong = client.get("/thing", headers={"Authorization": "Bearer nope"})
            basic = client.get("/thing", headers={"Authorization": "Basic abc"})
        assert missing.status_code == wrong.status_code == basic.status_code == 401
        assert missing.headers["www-authenticate"].startswith("Bearer")
        assert missing.json()["detail"].startswith("missing bearer token")
        assert wrong.json()["detail"].startswith("invalid bearer token")
        reasons = [
            r.__dict__["reason"]
            for r in caplog.records
            if r.__dict__.get("event") == "auth_rejected"
        ]
        assert reasons == ["missing", "invalid", "missing"]
        assert not any("nope" in r.getMessage() for r in caplog.records)

        ok = client.get("/thing", headers={"Authorization": f"bearer {TOKEN}"})
        assert ok.status_code == 200 and ok.json() == {"n": 1}

    def test_health_metrics_and_preflight_stay_open(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
        client = TestClient(_app())
        assert client.get("/health").status_code == 200
        assert client.get("/metrics").status_code == 200
        preflight = client.options(
            "/thing",
            headers={"Origin": "http://ui", "Access-Control-Request-Method": "GET"},
        )
        assert preflight.status_code == 200

    def test_401_keeps_cors_headers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
        response = TestClient(_app()).get("/thing", headers={"Origin": "http://ui"})
        assert response.status_code == 401
        assert response.headers["access-control-allow-origin"] == "*"

    def test_install_logs_whether_it_is_on(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
        with caplog.at_level(logging.INFO, logger="reflowfy.core.api_auth"):
            _app()
        (record,) = [r for r in caplog.records if r.__dict__.get("event") == "auth_configured"]
        assert record.__dict__["enabled"] is True
        assert TOKEN not in record.getMessage()

    def test_auth_headers_and_executor_client(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
        assert auth_headers() == {"Authorization": f"Bearer {TOKEN}"}
        executor = DistributedExecutor(reflow_manager_url="http://manager:8001")
        assert executor._get_client().headers["authorization"] == f"Bearer {TOKEN}"


def test_api_service_is_guarded(monkeypatch: pytest.MonkeyPatch) -> None:
    from reflowfy.api.app import create_app

    monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
    client = TestClient(create_app())  # no lifespan: no discovery
    assert client.get("/pipelines").status_code == 401
    assert client.get("/health").status_code == 200
    authed = client.get("/pipelines", headers={"Authorization": f"Bearer {TOKEN}"})
    assert authed.status_code == 200


# ----- manager routes ---------------------------------------------------------------


@pytest.fixture
def factory(tmp_path: Any) -> Iterator[sessionmaker[Session]]:
    engine = create_engine(
        f"sqlite:///{tmp_path}/auth.db", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


@pytest.fixture
def client(factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.delenv(API_TOKEN_ENV, raising=False)

    def db_override() -> Iterator[Session]:
        session = factory()
        try:
            yield session
        finally:
            session.close()

    def manager_override() -> Iterator[ReflowManager]:
        session = factory()
        try:
            yield ReflowManager(session, execution_mode="local")
        finally:
            session.close()

    app_module.app.dependency_overrides[get_db] = db_override
    app_module.app.dependency_overrides[app_module.get_reflow_manager] = manager_override
    yield TestClient(app_module.app)
    app_module.app.dependency_overrides.clear()


SECRET_PARAMS = {"db_password": "pw1", "day": "2026-01-01"}
PAYLOAD = {
    "schema_version": 2,
    "job_id": "j1",
    "source": {
        "type": "SqlSource",
        "config": {"connection_url": "postgresql://u:pw2@h/db", "query": "SELECT 1"},
    },
    "metadata": {"runtime_params": SECRET_PARAMS},
}
NOW = datetime(2026, 1, 1)


def _seed(factory: sessionmaker[Session], state: str = "running") -> None:
    with factory() as db:
        db.add(
            Execution(
                execution_id="e",
                pipeline_name="p",
                state=state,
                plan_complete=True,
                runtime_params=SECRET_PARAMS,
            )
        )
        db.add(Job(job_id="j1", execution_id="e", job_payload=PAYLOAD, state="failed"))
        db.add(
            DLQJob(
                job_payload=SECRET_PARAMS,
                pipeline_name="p",
                scheduled_at=NOW,
                delay_minutes=0,
                status="pending",
                retry_count=0,
                max_retries=3,
                created_at=NOW,
                updated_at=NOW,
            )
        )
        db.add(
            DLQJobArchive(
                job_payload=SECRET_PARAMS,
                pipeline_name="p",
                delay_minutes=0,
                retry_count=3,
                max_retries=3,
                created_at=NOW,
                archived_at=NOW,
            )
        )
        db.add(
            PipelineSchedule(
                pipeline_name="p",
                schedule_name="nightly",
                cron_expression="0 0 * * *",
                next_run_at=NOW,
                runtime_params=SECRET_PARAMS,
            )
        )
        db.commit()


def _assert_clean(body: Any) -> None:
    text = str(body)
    assert "pw1" not in text and "pw2" not in text, text


def test_jobs_are_redacted(client: TestClient, factory: sessionmaker[Session]) -> None:
    _seed(factory)
    (job,) = client.get("/executions/e/jobs").json()
    _assert_clean(job)
    assert job["job_payload"]["source"]["config"]["connection_url"] == (
        f"postgresql://u:{REDACTED}@h/db"
    )
    assert job["job_payload"]["metadata"]["runtime_params"] == {
        "db_password": REDACTED,
        "day": "2026-01-01",
    }


def test_execution_responses_are_redacted(
    client: TestClient, factory: sessionmaker[Session]
) -> None:
    _seed(factory)
    paused = client.post("/executions/e/pause").json()
    _assert_clean(paused)
    assert paused["execution"]["runtime_params"]["db_password"] == REDACTED
    _assert_clean(client.post("/executions/e/cancel").json())


def test_execution_stats_are_redacted(client: TestClient, factory: sessionmaker[Session]) -> None:
    _seed(factory)
    body = client.get("/executions/e/stats")
    assert body.status_code == 200, body.text
    _assert_clean(body.json())
    assert body.json()["runtime_params"] == {"db_password": REDACTED, "day": "2026-01-01"}


def test_job_errors_hide_url_passwords(client: TestClient, factory: sessionmaker[Session]) -> None:
    _seed(factory)
    with factory() as db:
        job = db.get(Job, "j1")
        assert job is not None
        job.error_message = "could not connect to postgresql://u:pw2@h/db"
        job.error_traceback = "Traceback ...\nOperationalError: postgresql://u:pw2@h/db"
        db.commit()
    (error,) = client.get("/executions/e/errors").json()
    _assert_clean(error)
    assert error["error_message"] == f"could not connect to postgresql://u:{REDACTED}@h/db"


def test_state_override_is_redacted(client: TestClient, factory: sessionmaker[Session]) -> None:
    _seed(factory)
    body = client.patch("/executions/e/state", json={"state": "failed", "error_message": "x"})
    assert body.status_code == 200, body.text
    _assert_clean(body.json())


def test_dlq_schedule_and_archive_rows_are_redacted(
    client: TestClient, factory: sessionmaker[Session]
) -> None:
    _seed(factory)
    for path in (
        "/dlq/jobs",
        "/dlq/jobs/1",
        "/archive/jobs",
        "/archive/jobs/1",
        "/schedules",
        "/schedules/p",
    ):
        response = client.get(path)
        assert response.status_code == 200, (path, response.text)
        _assert_clean(response.json())


def test_manager_requires_the_token(
    client: TestClient, factory: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(factory)
    monkeypatch.setenv(API_TOKEN_ENV, TOKEN)
    assert client.get("/executions/e/jobs").status_code == 401
    assert client.get("/metrics").status_code == 200
    ok = client.get("/executions/e/jobs", headers={"Authorization": f"Bearer {TOKEN}"})
    assert ok.status_code == 200


def test_helm_and_compose_wire_the_token() -> None:
    from pathlib import Path

    root = Path(__file__).parents[2] / "reflowfy"
    templates = root / "helm/reflowfy/templates"
    include = '{{- include "reflowfy.auth.env" . | nindent 12 }}'
    assert include in (templates / "api.yaml").read_text()
    assert include in (templates / "reflow-manager.yaml").read_text()
    assert include not in (templates / "worker.yaml").read_text()  # workers serve no API
    assert 'define "reflowfy.auth.env"' in (templates / "_helpers.tpl").read_text()
    assert "-api-token" in (templates / "secret.yaml").read_text()
    assert "apiToken: ''" in (root / "helm/reflowfy/values.yaml").read_text()
    compose = (root / "templates/docker-compose.yml").read_text()
    assert compose.count("REFLOWFY_API_TOKEN: ${REFLOWFY_API_TOKEN:-}") == 2


def test_destination_repr_hides_credentials() -> None:
    from reflowfy.destinations.api import api_destination
    from reflowfy.destinations.kafka import kafka_destination

    kafka = kafka_destination(
        topic="t",
        bootstrap_servers="k:9092",
        security_protocol="SASL_SSL",
        sasl_username="u",
        sasl_password="pw1",
    )
    api = api_destination(
        url="https://u:pw2@h/x", auth_token="pw1", headers={"Authorization": "Bearer pw1"}
    )
    for destination in (kafka, api):
        assert "pw1" not in repr(destination) and "pw2" not in repr(destination), repr(destination)


def test_free_text_fields_lose_url_passwords():
    from reflowfy.reflow_manager.redaction import redact_row

    row = redact_row(
        {
            "error_message": "could not connect to postgresql://app:hunter2@db:5432/x",
            "hook_error": "callback to https://u:p4ss@hooks.example failed",
            "state": "failed",
        }
    )
    assert "hunter2" not in row["error_message"] and "p4ss" not in row["hook_error"]
    assert row["state"] == "failed"


def test_routes_return_stored_rows_only_through_public_row():
    """A row's to_dict() handed out directly skips redaction (the leak public_row
    exists for). Pipeline metadata (pipeline.to_dict()) holds no stored values."""
    import ast
    import pathlib

    import reflowfy.reflow_manager as pkg

    allowed_receivers = {"pipeline", "p", "pipeline_obj", "job_retry", "context", "summary"}
    offenders = []
    for path in pathlib.Path(pkg.__file__).parent.glob("*.py"):
        if path.name == "redaction.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "to_dict"
            ):
                receiver = node.func.value
                name = receiver.id if isinstance(receiver, ast.Name) else ast.unparse(receiver)
                if name not in allowed_receivers:
                    offenders.append(f"{path.name}:{node.lineno} {name}.to_dict()")
    assert offenders == []
