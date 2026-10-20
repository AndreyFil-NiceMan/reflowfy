"""Dynamic /pipelines/{name}/run routes: schema, validation and forwarding."""

import json
import types
from typing import Any, Dict, Iterator, List, Literal
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from typing_extensions import Annotated, NotRequired, Required

from reflowfy import AbstractPipeline, Param, RuntimeParams, pipeline_registry
from reflowfy.api.execution import execution_tracker
from reflowfy.api.routes import _create_run_route, create_pipeline_routes
from reflowfy.core.abstract_pipeline import PipelineParameter
from reflowfy.execution.base import ExecutionState, ExecutionStatus
from reflowfy.execution.distributed_executor import DistributedExecutor


class RouteParams(RuntimeParams, total=False):
    env: Required[Literal["dev", "prod"]]
    limit: Annotated[NotRequired[int], Param("Max rows", default=100)]
    tags: Required[List[str]]
    options: NotRequired[Dict[str, Any]]


class ScalarParams(RuntimeParams, total=False):
    env: Annotated[NotRequired[Literal["dev", "prod"]], Param(default="dev")]
    verbose: NotRequired[bool]


class OptionalBodyParams(RuntimeParams, total=False):
    tags: NotRequired[List[str]]


def _define(name: str, params_type: Any, **attrs: Any) -> AbstractPipeline[Any]:
    """Define (and so register) a throwaway pipeline class; return its instance."""

    def define_source(self, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_transformations(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return []

    def define_destination(self, records, runtime_params):  # type: ignore[no-untyped-def]
        return None

    namespace = {
        "name": name,
        "define_source": define_source,
        "define_transformations": define_transformations,
        "define_destination": define_destination,
        **attrs,
    }
    types.new_class(
        f"_{name}", (AbstractPipeline[params_type],), {}, lambda ns: ns.update(namespace)
    )
    pipeline = pipeline_registry.get(name)
    assert pipeline is not None
    return pipeline


@pytest.fixture
def make_pipeline() -> Iterator[Any]:
    names: List[str] = []

    def factory(name: str, params_type: Any, **attrs: Any) -> AbstractPipeline[Any]:
        names.append(name)
        return _define(name, params_type, **attrs)

    yield factory
    for name in names:
        pipeline_registry.unregister(name)


@pytest.fixture(autouse=True)
def _clean_tracker() -> Iterator[None]:
    yield
    execution_tracker.clear()


def _executor(state: ExecutionState = ExecutionState.RUNNING, error: str = "") -> MagicMock:
    executor = MagicMock()

    def execute(pipeline, runtime_params, rate_limit_override=None):  # type: ignore[no-untyped-def]
        return ExecutionStatus(
            execution_id="exec-1",
            pipeline_name=pipeline.name,
            state=state,
            total_jobs=4,
            error_message=error or None,
        )

    executor.execute.side_effect = execute
    return executor


def _app(pipeline: Any, local: Any = None, distributed: Any = None) -> FastAPI:
    app = FastAPI()
    create_pipeline_routes(app, pipeline, local or _executor(), distributed or _executor())
    return app


# ---------------------------------------------------------------------------
# OpenAPI shape
# ---------------------------------------------------------------------------


class TestRunRouteSchema:
    def test_scalar_params_are_typed_query_params(self, make_pipeline):
        pipeline = make_pipeline("routes_schema_typed", RouteParams, rate_limit=60)
        op = _app(pipeline).openapi()["paths"]["/pipelines/routes_schema_typed/run"]["post"]
        params = {p["name"]: p for p in op["parameters"]}

        assert set(params) == {"mode", "rate_limit", "env", "limit"}
        assert all(p["in"] == "query" for p in params.values())

        env = params["env"]
        assert env["required"] is True
        assert env["schema"]["enum"] == ["dev", "prod"]

        limit = params["limit"]
        assert limit.get("required", False) is False
        assert limit["schema"]["type"] == "integer"
        assert limit["schema"]["default"] == 100
        assert limit["description"] == "Max rows"

        mode = params["mode"]
        assert mode["schema"]["enum"] == ["local", "distributed"]
        assert mode["schema"]["default"] == "distributed"
        assert "Pipeline default: 60" in params["rate_limit"]["description"]

    def test_list_and_dict_params_go_in_the_body(self, make_pipeline):
        pipeline = make_pipeline("routes_schema_body", RouteParams)
        spec = _app(pipeline).openapi()
        op = spec["paths"]["/pipelines/routes_schema_body/run"]["post"]

        assert op["requestBody"]["required"] is True
        ref = op["requestBody"]["content"]["application/json"]["schema"]["$ref"]
        model = spec["components"]["schemas"][ref.rsplit("/", 1)[-1]]
        assert set(model["properties"]) == {"tags", "options"}
        assert model["required"] == ["tags"]
        assert model["properties"]["tags"]["type"] == "array"
        assert model["properties"]["tags"]["description"] == "Runtime parameter: tags"

    def test_scalar_only_pipeline_has_no_request_body(self, make_pipeline):
        pipeline = make_pipeline("routes_schema_scalar", ScalarParams)
        op = _app(pipeline).openapi()["paths"]["/pipelines/routes_schema_scalar/run"]["post"]
        assert "requestBody" not in op
        env = next(p for p in op["parameters"] if p["name"] == "env")
        assert env["schema"]["default"] == "dev"
        assert env["schema"]["enum"] == ["dev", "prod"]

    @pytest.mark.parametrize("reserved", ["mode", "rate_limit", "body"])
    def test_reserved_parameter_names_are_rejected(self, reserved):
        pipeline = MagicMock()
        pipeline.name = "routes_reserved"
        params = [PipelineParameter(name=reserved, param_type=str)]
        with pytest.raises(ValueError, match=f"'{reserved}'"):
            _create_run_route(FastAPI(), pipeline, None, None, params)

    def test_status_route_describes_the_pipeline(self, make_pipeline):
        pipeline = make_pipeline(
            "routes_status", ScalarParams, description="Moves rows.", rate_limit=30
        )
        resp = TestClient(_app(pipeline)).get("/pipelines/routes_status/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "routes_status"
        assert body["description"] == "Moves rows."
        assert body["rate_limit"] == 30
        assert body["transformations"] == []
        assert body["runtime_parameters"] == ["env", "verbose"]


# ---------------------------------------------------------------------------
# Request validation and _run_body
# ---------------------------------------------------------------------------


class TestRunRouteRequests:
    def test_local_mode_uses_local_executor_with_merged_params(self, make_pipeline):
        local, distributed = _executor(), _executor()
        pipeline = make_pipeline("routes_run_local", RouteParams)
        client = TestClient(_app(pipeline, local, distributed))

        resp = client.post(
            "/pipelines/routes_run_local/run?mode=local&env=prod",
            json={"tags": ["a", "b"]},
        )

        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["execution_id"] == "exec-1"
        assert body["mode"] == "local"
        assert body["pipeline_name"] == "routes_run_local"
        assert body["rate_limit"] is None
        assert body["status"]["total_jobs"] == 4
        distributed.execute.assert_not_called()
        kwargs = local.execute.call_args.kwargs
        assert kwargs["pipeline"] is pipeline
        assert kwargs["rate_limit_override"] is None
        assert kwargs["runtime_params"] == {
            "env": "prod",
            "limit": 100,
            "tags": ["a", "b"],
            "options": None,
        }
        assert execution_tracker.get_status("exec-1") is not None

    def test_default_mode_is_distributed_and_rate_limit_is_forwarded(self, make_pipeline):
        local, distributed = _executor(), _executor()
        pipeline = make_pipeline("routes_run_dist", ScalarParams)
        client = TestClient(_app(pipeline, local, distributed))

        resp = client.post("/pipelines/routes_run_dist/run?rate_limit=120&verbose=true")

        assert resp.status_code == 200, resp.text
        assert resp.json()["mode"] == "distributed"
        assert resp.json()["rate_limit"] == 120
        local.execute.assert_not_called()
        kwargs = distributed.execute.call_args.kwargs
        assert kwargs["rate_limit_override"] == 120
        assert kwargs["runtime_params"] == {"env": "dev", "verbose": True}

    @pytest.mark.parametrize(
        "query, body",
        [
            ("", {"tags": ["a"]}),  # missing required env
            ("env=staging", {"tags": ["a"]}),  # not one of the Literal choices
            ("env=dev", {}),  # missing required body field
            ("env=dev&limit=many", {"tags": ["a"]}),  # wrong scalar type
            ("env=dev&mode=cluster", {"tags": ["a"]}),  # bad mode
        ],
    )
    def test_invalid_requests_are_rejected_before_execution(self, make_pipeline, query, body):
        local, distributed = _executor(), _executor()
        pipeline = make_pipeline("routes_invalid", RouteParams)
        client = TestClient(_app(pipeline, local, distributed))

        resp = client.post(f"/pipelines/routes_invalid/run?{query}", json=body)

        assert resp.status_code == 422
        local.execute.assert_not_called()
        distributed.execute.assert_not_called()

    def test_executor_exception_is_a_500_with_the_message(self, make_pipeline):
        broken = MagicMock()
        broken.execute.side_effect = RuntimeError("manager exploded")
        pipeline = make_pipeline("routes_raises", ScalarParams)
        client = TestClient(_app(pipeline, distributed=broken))

        resp = client.post("/pipelines/routes_raises/run")

        assert resp.status_code == 500
        assert resp.json()["detail"] == "manager exploded"

    def test_failed_execution_is_a_422_naming_the_execution(self, make_pipeline):
        failing = _executor(ExecutionState.FAILED, error="source unreachable")
        pipeline = make_pipeline("routes_failed", ScalarParams)
        client = TestClient(_app(pipeline, distributed=failing))

        resp = client.post("/pipelines/routes_failed/run")

        assert resp.status_code == 422
        assert resp.json()["detail"] == {
            "error": "source unreachable",
            "execution_id": "exec-1",
        }
        # Tracked even though it failed, so /executions/{id}/status can find it.
        assert execution_tracker.get_status("exec-1").state == ExecutionState.FAILED

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "BUG: a pipeline whose only list/dict params are optional still requires a "
            "JSON body (the body Parameter defaults to ...), so POST without one is 422"
        ),
    )
    def test_optional_body_params_do_not_require_a_body(self, make_pipeline):
        pipeline = make_pipeline("routes_optional_body", OptionalBodyParams)
        client = TestClient(_app(pipeline))
        resp = client.post("/pipelines/routes_optional_body/run")
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# Forwarding to the ReflowManager (real DistributedExecutor, mocked httpx)
# ---------------------------------------------------------------------------


def _distributed(handler: Any, mode: str = "distributed") -> DistributedExecutor:
    executor = DistributedExecutor(reflow_manager_url="http://manager:8001/", mode=mode)
    executor._client = httpx.Client(transport=httpx.MockTransport(handler))
    return executor


class TestForwardingToManager:
    def test_run_is_forwarded_to_manager_run(self, make_pipeline):
        seen: List[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json={"jobs_dispatched": 7})

        pipeline = make_pipeline("routes_forward", RouteParams)
        app = _app(pipeline, local=_distributed(handler, mode="local"))
        resp = TestClient(app).post(
            "/pipelines/routes_forward/run?mode=local&env=dev&rate_limit=30",
            json={"tags": ["x"], "options": {"k": 1}},
        )

        assert resp.status_code == 200, resp.text
        assert resp.json()["status"]["total_jobs"] == 7
        assert resp.json()["status"]["metadata"]["reflow_manager_url"] == "http://manager:8001"
        assert len(seen) == 1
        assert str(seen[0].url) == "http://manager:8001/run"
        sent = json.loads(seen[0].content)
        assert sent["pipeline_name"] == "routes_forward"
        assert sent["mode"] == "local"
        assert sent["rate_limit"] == 30
        assert sent["execution_id"] == resp.json()["execution_id"]
        assert sent["runtime_params"] == {
            "env": "dev",
            "limit": 100,
            "tags": ["x"],
            "options": {"k": 1},
        }

    def test_manager_error_response_fails_the_run(self, make_pipeline):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"detail": "unknown pipeline"})

        pipeline = make_pipeline("routes_manager_400", ScalarParams)
        resp = TestClient(_app(pipeline, distributed=_distributed(handler))).post(
            "/pipelines/routes_manager_400/run"
        )
        assert resp.status_code == 422
        assert "ReflowManager HTTP error" in resp.json()["detail"]["error"]

    def test_manager_unreachable_reports_the_manager_error(self, make_pipeline):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        pipeline = make_pipeline("routes_manager_down", ScalarParams)
        resp = TestClient(_app(pipeline, distributed=_distributed(handler))).post(
            "/pipelines/routes_manager_down/run"
        )
        assert resp.status_code >= 400
        assert "connection refused" in json.dumps(resp.json())

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "BUG: an unreachable ReflowManager is reported as 422 Unprocessable Entity "
            "(blaming the caller's input) instead of a 502/503 upstream error"
        ),
    )
    def test_manager_unreachable_is_a_gateway_error(self, make_pipeline):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        pipeline = make_pipeline("routes_manager_down_5xx", ScalarParams)
        resp = TestClient(_app(pipeline, distributed=_distributed(handler))).post(
            "/pipelines/routes_manager_down_5xx/run"
        )
        assert resp.status_code in (502, 503)
