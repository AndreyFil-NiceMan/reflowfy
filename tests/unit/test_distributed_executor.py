"""DistributedExecutor: forwarding /run to the ReflowManager over a mocked HTTP
transport (httpx.MockTransport), and the get_executor factory."""

import json
import uuid
from types import SimpleNamespace
from typing import Any, Callable, Dict, List

import httpx
import pytest

from reflowfy.execution.base import ExecutionState
from reflowfy.execution.distributed_executor import DistributedExecutor, get_executor
from reflowfy.execution.local_executor import LocalExecutor

PIPELINE = SimpleNamespace(name="orders")


def _executor(
    handler: Callable[[httpx.Request], httpx.Response], **kwargs: Any
) -> "tuple[DistributedExecutor, List[httpx.Request]]":
    requests: List[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    ex = DistributedExecutor(**kwargs)
    ex._client = httpx.Client(transport=httpx.MockTransport(record))
    return ex, requests


class TestExecute:
    def test_success_forwards_the_run_and_reports_dispatched_jobs(self):
        ex, requests = _executor(
            lambda r: httpx.Response(200, json={"jobs_dispatched": 7}),
            reflow_manager_url="http://manager:8001/",
            mode="local",
        )

        status = ex.execute(
            PIPELINE, {"env": "prod"}, execution_id="exec-1", rate_limit_override=600.0
        )

        assert status.state == ExecutionState.RUNNING  # jobs run asynchronously
        assert (status.execution_id, status.pipeline_name) == ("exec-1", "orders")
        assert status.total_jobs == 7
        assert status.metadata == {
            "jobs_dispatched": 7,
            "reflow_manager_url": "http://manager:8001",
        }
        assert status.error_message is None
        (request,) = requests
        assert request.method == "POST"
        assert str(request.url) == "http://manager:8001/run"  # trailing slash stripped
        assert json.loads(request.content) == {
            "pipeline_name": "orders",
            "runtime_params": {"env": "prod"},
            "execution_id": "exec-1",
            "rate_limit": 600.0,
            "mode": "local",
        }

    def test_execution_id_is_generated_when_omitted(self):
        ex, requests = _executor(lambda r: httpx.Response(200, json={"jobs_dispatched": 0}))

        status = ex.execute(PIPELINE, {})

        uuid.UUID(status.execution_id)  # a valid uuid
        assert json.loads(requests[0].content)["execution_id"] == status.execution_id
        assert json.loads(requests[0].content)["rate_limit"] is None
        assert json.loads(requests[0].content)["mode"] == "distributed"

    def test_missing_jobs_dispatched_counts_as_zero(self):
        ex, _ = _executor(lambda r: httpx.Response(200, json={}))
        status = ex.execute(PIPELINE, {})
        assert status.total_jobs == 0
        assert status.state == ExecutionState.RUNNING

    @pytest.mark.parametrize("code", [400, 404, 422, 500, 503])
    def test_error_status_fails_the_execution(self, code):
        ex, _ = _executor(lambda r: httpx.Response(code, json={"detail": "nope"}))

        status = ex.execute(PIPELINE, {}, execution_id="e")

        assert status.state == ExecutionState.FAILED
        assert status.error_message is not None
        assert status.error_message.startswith("ReflowManager HTTP error:")
        assert str(code) in status.error_message
        assert status.total_jobs == 0

    def test_timeout_fails_the_execution(self):
        def timeout(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("timed out", request=request)

        ex, _ = _executor(timeout)
        status = ex.execute(PIPELINE, {})

        assert status.state == ExecutionState.FAILED
        assert status.error_message == "ReflowManager HTTP error: timed out"

    def test_unreachable_manager_fails_the_execution(self):
        def refused(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        ex, _ = _executor(refused)
        status = ex.execute(PIPELINE, {})

        assert status.state == ExecutionState.FAILED
        assert "connection refused" in (status.error_message or "")

    def test_non_json_body_fails_the_execution(self):
        ex, _ = _executor(lambda r: httpx.Response(200, text="<html>proxy</html>"))
        status = ex.execute(PIPELINE, {})

        assert status.state == ExecutionState.FAILED
        assert (status.error_message or "").startswith("Failed to run pipeline:")

    def test_unserialisable_params_fail_instead_of_raising(self):
        ex, requests = _executor(lambda r: httpx.Response(200, json={}))
        status = ex.execute(PIPELINE, {"bad": object()})

        assert status.state == ExecutionState.FAILED
        assert (status.error_message or "").startswith("Failed to run pipeline:")
        assert requests == []


class TestClient:
    def test_client_is_created_once_with_the_timeout(self):
        ex = DistributedExecutor(timeout=12.5)
        client = ex._get_client()
        try:
            assert ex._get_client() is client
            assert client.timeout.read == 12.5
        finally:
            ex.close()

    def test_close_is_idempotent_and_a_new_client_follows(self):
        ex = DistributedExecutor()
        first = ex._get_client()
        ex.close()
        ex.close()
        assert first.is_closed
        second = ex._get_client()
        assert second is not first
        ex.close()


class TestGetExecutor:
    def test_local(self):
        ex = get_executor("local", max_records=5)
        assert isinstance(ex, LocalExecutor)

    def test_distributed_with_defaults(self):
        ex = get_executor("distributed")
        assert isinstance(ex, DistributedExecutor)
        assert (ex.reflow_manager_url, ex.timeout, ex.mode) == (
            "http://localhost:8001",
            30.0,
            "distributed",
        )

    def test_distributed_with_options(self):
        ex = get_executor(
            "distributed",
            reflow_manager_url="http://m:9/",
            timeout=3.0,
            execution_mode="local",
        )
        assert isinstance(ex, DistributedExecutor)
        assert (ex.reflow_manager_url, ex.timeout, ex.mode) == ("http://m:9", 3.0, "local")

    def test_unknown_mode(self):
        with pytest.raises(ValueError, match="Unknown execution mode: batch"):
            get_executor("batch")


def test_status_payload_shape_is_stable() -> None:
    """Guard the request body keys the manager's /run route reads."""
    ex, requests = _executor(lambda r: httpx.Response(200, json={"jobs_dispatched": 1}))
    ex.execute(PIPELINE, {"a": 1}, execution_id="x")
    body: Dict[str, Any] = json.loads(requests[0].content)
    assert set(body) == {"pipeline_name", "runtime_params", "execution_id", "rate_limit", "mode"}
