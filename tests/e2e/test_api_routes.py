"""
E2E Tests for API dynamic route generation.

Tests the three fixes made to reflowfy/api/routes.py:
1. Typed query parameters (bool, int, float) are correctly parsed
2. IdBasedPipeline exposes `ids` in the request body (not a query param)
3. Pipeline failures return HTTP 422 with an error message

Uses FastAPI's TestClient — no running services required.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from reflowfy.api.routes import create_pipeline_routes
from reflowfy.core.abstract_pipeline import AbstractPipeline, PipelineParameter
from reflowfy.core.id_based_pipeline import IdBasedPipeline
from reflowfy.execution.base import ExecutionState, ExecutionStatus

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_status(state: ExecutionState, error: str | None = None) -> ExecutionStatus:
    return ExecutionStatus(
        execution_id="test-exec-id",
        pipeline_name="test",
        state=state,
        total_jobs=1,
        completed_jobs=1 if state == ExecutionState.COMPLETED else 0,
        failed_jobs=1 if state == ExecutionState.FAILED else 0,
        error_message=error,
    )


def _make_executor(state: ExecutionState = ExecutionState.COMPLETED, error: str | None = None):
    executor = MagicMock()
    executor.execute.return_value = _make_status(state, error)
    return executor


def _build_app(*pipelines) -> tuple[FastAPI, MagicMock, MagicMock]:
    """Return (app, local_executor, distributed_executor) with pipelines registered."""
    app = FastAPI()
    local_ex = _make_executor()
    dist_ex = _make_executor()
    for pipeline in pipelines:
        create_pipeline_routes(app, pipeline, local_ex, dist_ex)
    return app, local_ex, dist_ex


# ---------------------------------------------------------------------------
# Test Pipelines
# ---------------------------------------------------------------------------


class _NoParmsPipeline(AbstractPipeline):
    name = "_test_no_params"

    def define_parameters(self):
        return []

    def define_source(self, p):
        return MagicMock()

    def define_destination(self, p):
        return MagicMock()

    def define_transformations(self, p):
        return []


class _TypedParamsPipeline(AbstractPipeline):
    name = "_test_typed_params"

    def define_parameters(self):
        return [
            PipelineParameter(
                name="flag", param_type=bool, required=True, description="A boolean flag"
            ),
            PipelineParameter(name="count", param_type=int, required=False, default=10),
            PipelineParameter(name="threshold", param_type=float, required=False, default=0.5),
            PipelineParameter(name="label", param_type=str, required=False, default="default"),
        ]

    def define_source(self, p):
        return MagicMock()

    def define_destination(self, p):
        return MagicMock()

    def define_transformations(self, p):
        return []


class _IdBasedTestPipeline(IdBasedPipeline):
    name = "_test_id_based"

    def define_parameters(self):
        return [
            PipelineParameter(name="env", param_type=str, required=False, default="prod"),
        ]

    def define_source(self, p, ids):
        return MagicMock()

    def define_destination(self, p):
        return MagicMock()

    def define_transformations(self, p, ids):
        return []


class _IdBasedNoExtraParamsPipeline(IdBasedPipeline):
    name = "_test_id_based_no_extra"

    def define_parameters(self):
        return []

    def define_source(self, p, ids):
        return MagicMock()

    def define_destination(self, p):
        return MagicMock()

    def define_transformations(self, p, ids):
        return []


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def no_params_client():
    app, local, dist = _build_app(_NoParmsPipeline())
    return TestClient(app), local, dist


@pytest.fixture(scope="module")
def typed_client():
    app, local, dist = _build_app(_TypedParamsPipeline())
    return TestClient(app), local, dist


@pytest.fixture(scope="module")
def id_based_client():
    app, local, dist = _build_app(_IdBasedTestPipeline())
    return TestClient(app), local, dist


@pytest.fixture(scope="module")
def id_based_no_extra_client():
    app, local, dist = _build_app(_IdBasedNoExtraParamsPipeline())
    return TestClient(app), local, dist


# ---------------------------------------------------------------------------
# 1. No-params pipeline
# ---------------------------------------------------------------------------


class TestNoParmsPipeline:
    def test_run_returns_200(self, no_params_client):
        client, _, _ = no_params_client
        r = client.post("/_test_no_params/run", params={"mode": "local"})
        # Route is registered as /pipelines/<name>/run
        r2 = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        assert r2.status_code == 200

    def test_run_response_shape(self, no_params_client):
        client, _, _ = no_params_client
        r = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        body = r.json()
        assert body["pipeline_name"] == "_test_no_params"
        assert body["mode"] == "local"
        assert "execution_id" in body
        assert body["status"]["state"] == "completed"

    def test_status_endpoint(self, no_params_client):
        client, _, _ = no_params_client
        r = client.get("/pipelines/_test_no_params/status")
        assert r.status_code == 200
        assert r.json()["name"] == "_test_no_params"

    def test_distributed_mode_uses_dist_executor(self, no_params_client):
        client, local_ex, dist_ex = no_params_client
        local_ex.reset_mock()
        dist_ex.reset_mock()
        client.post("/pipelines/_test_no_params/run", params={"mode": "distributed"})
        assert dist_ex.execute.called
        assert not local_ex.execute.called

    def test_local_mode_uses_local_executor(self, no_params_client):
        client, local_ex, dist_ex = no_params_client
        local_ex.reset_mock()
        dist_ex.reset_mock()
        client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        assert local_ex.execute.called
        assert not dist_ex.execute.called


# ---------------------------------------------------------------------------
# 2. Typed query parameters (Fix 1: bool / int / float correctly parsed)
# ---------------------------------------------------------------------------


class TestTypedQueryParams:
    def test_bool_true_accepted(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true"},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["flag"] is True

    def test_bool_false_accepted(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "false"},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["flag"] is False

    def test_invalid_bool_rejected(self, typed_client):
        client, _, _ = typed_client
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "not_a_bool"},
        )
        assert r.status_code == 422

    def test_int_param_parsed(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true", "count": "42"},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["count"] == 42
        assert isinstance(kwargs["runtime_params"]["count"], int)

    def test_invalid_int_rejected(self, typed_client):
        client, _, _ = typed_client
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true", "count": "not_an_int"},
        )
        assert r.status_code == 422

    def test_float_param_parsed(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true", "threshold": "0.75"},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert abs(kwargs["runtime_params"]["threshold"] - 0.75) < 1e-9

    def test_str_param_passed_through(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true", "label": "staging"},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["label"] == "staging"

    def test_missing_required_param_rejected(self, typed_client):
        client, _, _ = typed_client
        # `flag` is required — omitting it must return 422
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local"},
        )
        assert r.status_code == 422

    def test_optional_defaults_not_required(self, typed_client):
        client, local, _ = typed_client
        local.reset_mock()
        # Only provide the required `flag`
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true"},
        )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# 3. IdBasedPipeline — ids in request body (Fix 2)
# ---------------------------------------------------------------------------


class TestIdBasedRouteBody:
    def test_ids_in_body_accepted(self, id_based_client):
        client, local, _ = id_based_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": [1, 2, 3]},
        )
        assert r.status_code == 200

    def test_ids_passed_to_executor(self, id_based_client):
        client, local, _ = id_based_client
        local.reset_mock()
        client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": [10, 20, 30]},
        )
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["ids"] == [10, 20, 30]

    def test_extra_body_param_passed(self, id_based_client):
        client, local, _ = id_based_client
        local.reset_mock()
        client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": [1], "env": "staging"},
        )
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["env"] == "staging"

    def test_missing_ids_rejected(self, id_based_client):
        client, _, _ = id_based_client
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"env": "prod"},
        )
        assert r.status_code == 422

    def test_ids_as_string_in_query_param_rejected(self, id_based_client):
        """ids must not be accepted as a query string — body only."""
        client, _, _ = id_based_client
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local", "ids": "1,2,3"},
        )
        # Without a body providing ids, the request must fail (422)
        assert r.status_code == 422

    def test_ids_only_pipeline_no_extra_fields(self, id_based_no_extra_client):
        client, local, _ = id_based_no_extra_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_id_based_no_extra/run",
            params={"mode": "local"},
            json={"ids": ["a", "b"]},
        )
        assert r.status_code == 200
        _, kwargs = local.execute.call_args
        assert kwargs["runtime_params"]["ids"] == ["a", "b"]

    def test_ids_can_be_strings(self, id_based_client):
        client, local, _ = id_based_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": ["user-1", "user-2"]},
        )
        assert r.status_code == 200

    def test_empty_ids_list_accepted(self, id_based_client):
        client, local, _ = id_based_client
        local.reset_mock()
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": []},
        )
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# 4. Failure returns HTTP 422 (Fix 3)
# ---------------------------------------------------------------------------


class TestFailureResponse:
    def _make_failing_app(self, pipeline):
        app = FastAPI()
        local_ex = _make_executor(ExecutionState.FAILED, "Something went wrong")
        dist_ex = _make_executor(ExecutionState.FAILED, "Something went wrong")
        create_pipeline_routes(app, pipeline, local_ex, dist_ex)
        return TestClient(app)

    def test_no_params_pipeline_failure_returns_422(self):
        client = self._make_failing_app(_NoParmsPipeline())
        r = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        assert r.status_code == 422

    def test_failure_response_contains_error_message(self):
        client = self._make_failing_app(_NoParmsPipeline())
        r = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        body = r.json()
        assert "Something went wrong" in body["detail"]["error"]

    def test_failure_response_contains_execution_id(self):
        client = self._make_failing_app(_NoParmsPipeline())
        r = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        body = r.json()
        assert "execution_id" in body["detail"]

    def test_typed_params_pipeline_failure_returns_422(self):
        client = self._make_failing_app(_TypedParamsPipeline())
        r = client.post(
            "/pipelines/_test_typed_params/run",
            params={"mode": "local", "flag": "true"},
        )
        assert r.status_code == 422

    def test_id_based_pipeline_failure_returns_422(self):
        client = self._make_failing_app(_IdBasedTestPipeline())
        r = client.post(
            "/pipelines/_test_id_based/run",
            params={"mode": "local"},
            json={"ids": [1, 2, 3]},
        )
        assert r.status_code == 422

    def test_failure_not_200(self):
        """Regression: executor returning FAILED must never yield HTTP 200."""
        client = self._make_failing_app(_NoParmsPipeline())
        r = client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        assert r.status_code != 200


# ---------------------------------------------------------------------------
# 5. Rate limit forwarded to executor
# ---------------------------------------------------------------------------


class TestRateLimit:
    def test_rate_limit_passed_to_executor(self, no_params_client):
        client, local, _ = no_params_client
        local.reset_mock()
        client.post(
            "/pipelines/_test_no_params/run",
            params={"mode": "local", "rate_limit": "5.0"},
        )
        _, kwargs = local.execute.call_args
        assert kwargs["rate_limit_override"] == {"jobs_per_second": 5.0}

    def test_no_rate_limit_passes_none(self, no_params_client):
        client, local, _ = no_params_client
        local.reset_mock()
        client.post("/pipelines/_test_no_params/run", params={"mode": "local"})
        _, kwargs = local.execute.call_args
        assert kwargs["rate_limit_override"] is None
