"""The API service: startup discovery, built-in endpoints and route wiring."""

import logging
import types
from typing import Any, Dict, Iterator, List
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import reflowfy.api.app as api_app
from reflowfy import AbstractPipeline, RuntimeParams, __version__, pipeline_registry
from reflowfy.api.execution import execution_tracker


def _register(name: str, **attrs: Any) -> None:
    def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return None

    namespace: Dict[str, Any] = {
        "name": name,
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
        **attrs,
    }
    types.new_class(
        f"_{name}", (AbstractPipeline[RuntimeParams],), {}, lambda ns: ns.update(namespace)
    )


@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Run against an empty registry; restore whatever was there afterwards."""
    saved = dict(pipeline_registry._pipelines)
    pipeline_registry._pipelines.clear()
    try:
        yield
    finally:
        pipeline_registry._pipelines.clear()
        pipeline_registry._pipelines.update(saved)


@pytest.fixture
def executors(monkeypatch) -> List[Dict[str, Any]]:
    """Capture DistributedExecutor construction instead of building real HTTP clients."""
    calls: List[Dict[str, Any]] = []

    def fake_executor(**kwargs: Any) -> MagicMock:
        calls.append(kwargs)
        return MagicMock(name=f"executor-{kwargs.get('mode')}")

    monkeypatch.setattr(api_app, "DistributedExecutor", fake_executor)
    return calls


@pytest.fixture
def no_logging_setup(monkeypatch) -> None:
    monkeypatch.setattr(api_app, "setup_logging", lambda service_name="api": None)


@pytest.fixture(autouse=True)
def _clean_tracker() -> Iterator[None]:
    yield
    execution_tracker.clear()


class TestLifespan:
    def test_startup_discovers_pipelines_then_wires_their_routes(
        self, monkeypatch, isolated_registry, executors, no_logging_setup
    ):
        discovered: List[bool] = []

        def fake_discover() -> None:
            discovered.append(True)
            _register("app_discovered", rate_limit=50)

        monkeypatch.setattr(api_app, "discover_and_load_pipelines", fake_discover)
        monkeypatch.setenv("REFLOW_MANAGER_URL", "http://mgr:9001")

        app = api_app.create_app()
        assert discovered == []  # nothing happens until startup

        with TestClient(app) as client:
            assert discovered == [True]
            paths = app.openapi()["paths"]
            assert "/pipelines/app_discovered/run" in paths
            assert "/pipelines/app_discovered/status" not in paths

            listed = client.get("/pipelines").json()["pipelines"]
            assert listed == [
                {
                    "name": "app_discovered",
                    "folder": "",
                    "run_path": "/pipelines/app_discovered/run",
                    "transformations": [],
                    "runtime_parameters": [],
                    "rate_limit": 50,
                }
            ]

        # Both executors forward to the manager; only the dispatch mode differs.
        assert executors == [
            {"reflow_manager_url": "http://mgr:9001", "mode": "local"},
            {"reflow_manager_url": "http://mgr:9001", "mode": "distributed"},
        ]

    def test_startup_with_no_pipelines_adds_no_routes(
        self, monkeypatch, isolated_registry, executors, no_logging_setup, caplog
    ):
        monkeypatch.setattr(api_app, "discover_and_load_pipelines", lambda: None)
        app = api_app.create_app()
        with caplog.at_level(logging.WARNING, logger="reflowfy.api.app"):
            with TestClient(app) as client:
                assert client.get("/pipelines").json() == {"pipelines": []}
        assert "No pipelines registered" in caplog.text
        assert executors == []
        assert not [p for p in app.openapi()["paths"] if p.startswith("/pipelines/")]

    def test_a_pipeline_with_a_reserved_parameter_does_not_take_the_others_down(
        self, isolated_registry, executors, caplog
    ):
        from reflowfy import PipelineParameter

        _register(
            "app_reserved_param",
            define_parameters=lambda self: [PipelineParameter(name="mode")],
        )
        _register("app_fine_pipeline")
        app = FastAPI()
        with caplog.at_level(logging.ERROR, logger="reflowfy.api.app"):
            api_app.setup_pipeline_routes(app)
        paths = list(app.openapi()["paths"])
        assert "/pipelines/app_fine_pipeline/run" in paths
        assert "/pipelines/app_reserved_param/run" not in paths
        assert "Pipeline 'app_reserved_param' has no API route" in caplog.text

    def test_default_manager_url(self, monkeypatch, isolated_registry, executors):
        monkeypatch.delenv("REFLOW_MANAGER_URL", raising=False)
        _register("app_default_url")
        api_app.setup_pipeline_routes(FastAPI())
        assert {c["reflow_manager_url"] for c in executors} == {"http://localhost:8001"}


class TestBuiltinEndpoints:
    @pytest.fixture
    def client(self, monkeypatch, isolated_registry, no_logging_setup) -> Iterator[TestClient]:
        monkeypatch.setattr(api_app, "discover_and_load_pipelines", lambda: None)
        with TestClient(api_app.create_app()) as client:
            yield client

    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {
            "status": "healthy",
            "service": "reflowfy-api",
            "version": __version__,
        }

    def test_metrics_exposes_prometheus_text(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        assert "reflowfy_jobs_processed_total" in resp.text

    @staticmethod
    def _manager(monkeypatch, handler):
        """Route the status route's manager calls to ``handler`` (an httpx handler)."""
        real = httpx.AsyncClient
        monkeypatch.setenv("REFLOW_MANAGER_URL", "http://manager")
        monkeypatch.setattr(
            api_app.httpx,
            "AsyncClient",
            lambda **kw: real(transport=httpx.MockTransport(handler), **kw),
        )

    def test_execution_status_unknown_is_404(self, client, monkeypatch):
        self._manager(monkeypatch, lambda request: httpx.Response(404, json={}))
        resp = client.get("/executions/nope/status")
        assert resp.status_code == 404
        assert resp.json() == {"error": "Execution 'nope' not found"}

    def test_execution_status_is_read_live_from_the_manager(self, client, monkeypatch):
        """It used to be a snapshot of the 202 response that never changed."""
        seen: List[str] = []

        def manager(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url))
            return httpx.Response(
                200,
                json={
                    "execution_id": "known",
                    "pipeline_name": "p",
                    "state": "completed",
                    "total_jobs": 4,
                    "jobs_completed": 3,
                    "jobs_failed": 1,
                    "error_message": None,
                },
            )

        self._manager(monkeypatch, manager)
        resp = client.get("/executions/known/status")
        assert resp.status_code == 200
        body = resp.json()
        assert seen == ["http://manager/executions/known/stats"]
        assert body["state"] == "completed"
        assert (body["completed_jobs"], body["failed_jobs"]) == (3, 1)
        assert body["success_rate"] == 0.75

    def test_execution_status_when_the_manager_is_down_is_502(self, client, monkeypatch):
        def down(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused")

        self._manager(monkeypatch, down)
        assert client.get("/executions/x/status").status_code == 502

    def test_cors_allows_any_origin(self, client):
        resp = client.options(
            "/health",
            headers={"Origin": "http://example.com", "Access-Control-Request-Method": "GET"},
        )
        assert resp.status_code == 200
        assert resp.headers["access-control-allow-origin"] in ("*", "http://example.com")


class TestMain:
    def test_main_discovers_wires_and_serves_on_configured_host_port(
        self, monkeypatch, isolated_registry, executors, no_logging_setup
    ):
        import uvicorn

        served: Dict[str, Any] = {}
        monkeypatch.setattr(
            uvicorn, "run", lambda app, host, port: served.update(app=app, host=host, port=port)
        )
        monkeypatch.setattr(
            api_app, "discover_and_load_pipelines", lambda: _register("app_main_pipeline")
        )
        monkeypatch.setenv("API_HOST", "127.0.0.1")
        monkeypatch.setenv("API_PORT", "8123")

        api_app.main()

        assert served["host"] == "127.0.0.1"
        assert served["port"] == 8123
        assert isinstance(served["app"], FastAPI)
        assert "/pipelines/app_main_pipeline/run" in served["app"].openapi()["paths"]

    def test_main_defaults(self, monkeypatch, isolated_registry, executors, no_logging_setup):
        import uvicorn

        served: Dict[str, Any] = {}
        monkeypatch.setattr(
            uvicorn, "run", lambda app, host, port: served.update(host=host, port=port)
        )
        monkeypatch.setattr(api_app, "discover_and_load_pipelines", lambda: None)
        monkeypatch.delenv("API_HOST", raising=False)
        monkeypatch.delenv("API_PORT", raising=False)

        api_app.main()

        assert served == {"host": "0.0.0.0", "port": 8000}
