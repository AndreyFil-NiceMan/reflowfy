"""The API's client for the ReflowManager's ``/run``.

The API keeps no execution state of its own: it forwards a run to the manager and
reports what the manager said. ``mode`` is per call (``local`` makes the manager use
its in-process dispatcher), so one client serves every route.
"""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import httpx

from reflowfy.core.api_auth import auth_headers
from reflowfy.store.states import ExecutionState

logger = logging.getLogger(__name__)


#: Manager request timeout. /run answers 202 at once; the dispatch runs on the manager.
DEFAULT_TIMEOUT_SECONDS = 30.0


def _empty_metadata() -> Dict[str, Any]:
    """Typed default factory — a bare `dict` resolves to dict[Unknown, Unknown]."""
    return {}


@dataclass
class ExecutionStatus:
    """What the API reports for one submitted run (the ``status`` of its response).

    ``state`` is ``running`` once the manager accepted it, ``failed`` when it did not;
    the execution's real progress is the manager's (``/executions/{id}/status``).
    """

    execution_id: str
    pipeline_name: str
    state: str
    total_jobs: int = 0
    completed_jobs: int = 0
    failed_jobs: int = 0
    retry_count: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=_empty_metadata)

    @property
    def success_rate(self) -> float:
        if self.total_jobs == 0:
            return 0.0
        return self.completed_jobs / self.total_jobs

    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "pipeline_name": self.pipeline_name,
            "state": self.state,
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_jobs,
            "failed_jobs": self.failed_jobs,
            "retry_count": self.retry_count,
            "success_rate": self.success_rate,
            "error_message": self.error_message,
            "metadata": self.metadata,
        }


class DistributedExecutor:
    """Forwards a run to the ReflowManager's ``/run``, which plans, records and
    dispatches it (to Kafka, or in-process for ``mode="local"``)."""

    def __init__(
        self,
        reflow_manager_url: str = "http://localhost:8001",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        mode: str = "distributed",
    ):
        """
        Initialize distributed executor.

        Args:
            reflow_manager_url: ReflowManager service URL
            timeout: HTTP request timeout
            mode: Default execution mode ('local' or 'distributed'), when a call
                does not name one
        """
        self.reflow_manager_url = reflow_manager_url.rstrip("/")
        self.timeout = timeout
        self.mode = mode
        self._client: Optional[httpx.Client] = None

    def _get_client(self) -> httpx.Client:
        """Get or create HTTP client."""
        if self._client is None:
            # REFLOWFY_API_TOKEN, when set, guards the manager too.
            self._client = httpx.Client(timeout=self.timeout, headers=auth_headers())
        return self._client

    def execute(
        self,
        pipeline: Any,
        runtime_params: Dict[str, Any],
        execution_id: Optional[str] = None,
        rate_limit_override: Optional[float] = None,
        callback_url: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> ExecutionStatus:
        """
        Execute pipeline in distributed mode via ReflowManager.

        This simplified version just forwards the request to ReflowManager's /run endpoint.
        The ReflowManager handles:
        - Loading the pipeline
        - Splitting source data into jobs
        - Creating execution and checkpoints
        - Dispatching jobs to Kafka with rate limiting

        Args:
            pipeline: Pipeline instance
            runtime_params: Runtime parameters
            execution_id: Optional execution ID
            rate_limit_override: Optional jobs-per-minute override (e.g., 600)
            callback_url: Optional http(s) URL the manager POSTs the execution
                summary to when the run ends; validated by the manager

        Returns:
            ExecutionStatus (initial - jobs dispatched but processing async)
        """
        if execution_id is None:
            execution_id = str(uuid.uuid4())

        status = ExecutionStatus(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            state=ExecutionState.RUNNING,
        )

        try:
            logger.info(
                "Starting distributed execution %s (pipeline=%s, manager=%s)",
                execution_id,
                pipeline.name,
                self.reflow_manager_url,
                extra={
                    "event": "execution_submit",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline.name,
                },
            )

            client = self._get_client()

            rate_limit = rate_limit_override

            # Call ReflowManager's /run endpoint - it handles everything
            run_request: Dict[str, Any] = {
                "pipeline_name": pipeline.name,
                "runtime_params": runtime_params,
                "execution_id": execution_id,
                "rate_limit": rate_limit,
                "mode": mode or self.mode,
            }
            if callback_url is not None:
                run_request["callback_url"] = callback_url
            run_response = client.post(f"{self.reflow_manager_url}/run", json=run_request)

            # Handle errors
            if run_response.status_code >= 400:
                logger.error(
                    "Run failed for %s with status %d: %s",
                    execution_id,
                    run_response.status_code,
                    run_response.text,
                    extra={
                        "event": "execution_submit_rejected",
                        "execution_id": execution_id,
                        "pipeline_name": pipeline.name,
                        "http_status": run_response.status_code,
                    },
                )
                run_response.raise_for_status()

            result = run_response.json()

            total_jobs = result.get("jobs_dispatched", 0)

            logger.info(
                "Dispatched %d jobs via ReflowManager (execution %s)",
                total_jobs,
                execution_id,
                extra={
                    "event": "execution_submitted",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline.name,
                    "jobs_total": total_jobs,
                },
            )

            status.total_jobs = total_jobs
            status.metadata["jobs_dispatched"] = total_jobs
            status.metadata["reflow_manager_url"] = self.reflow_manager_url

            return status

        except httpx.HTTPError as e:
            error_msg = f"ReflowManager HTTP error: {e}"
            logger.error(
                "Execution %s: %s",
                execution_id,
                error_msg,
                extra={
                    "event": "execution_submit_error",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline.name,
                },
            )

            status.state = ExecutionState.FAILED
            status.error_message = error_msg
            # Unreachable, timed out or a 5xx: the manager failed, not the caller's
            # input. The API answers 502 for these instead of 422.
            if isinstance(e, httpx.TransportError) or (
                isinstance(e, httpx.HTTPStatusError) and e.response.status_code >= 500
            ):
                status.metadata["upstream_error"] = True

            return status

        except Exception as e:
            error_msg = f"Failed to run pipeline: {e}"
            logger.exception(
                "Execution %s: failed to run pipeline",
                execution_id,
                extra={
                    "event": "execution_submit_error",
                    "execution_id": execution_id,
                    "pipeline_name": pipeline.name,
                },
            )

            status.state = ExecutionState.FAILED
            status.error_message = error_msg

            return status

    def close(self):
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._client = None
