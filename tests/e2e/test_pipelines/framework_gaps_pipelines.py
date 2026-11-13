"""E2E pipelines for the framework-gaps features (PRs #105–#110).

Every pipeline sends to the mock API's ``/gaps/{run}/{name}`` endpoint
(``tests/e2e/destinations/mock_api_server.py``), which logs each request with its
time and can inject failures (``status``, ``fail_first``, ``retry_after``). ``run``
is a per-test key so tests never see each other's requests.

They run unchanged in local mode and, with ``scripts/e2e-distributed.override.yml``,
over Kafka with real workers.
"""

import os
from typing import Any, Dict, List

import httpx
from typing_extensions import Annotated, NotRequired

from reflowfy import (
    AbstractPipeline,
    ExecutionSummary,
    JobRetryConfig,
    Param,
    RetryConfig,
    RuntimeParams,
    api_destination,
    fan_out,
    job,
    record_destination,
    record_transformation,
    run_pipeline,
    secret,
    sql_source,
    transformation,
)

MOCK_BASE = os.getenv("MOCK_HTTP_URL", "http://reflofy-e2e-mock-http:8091/webhook").rsplit(
    "/webhook", 1
)[0]
SQL_URL = os.getenv(
    "SQL_CONNECTION_URL", "postgresql://reflowfy:reflowfy@postgres:5432/reflowfy"
)
FAST_RETRY = RetryConfig(max_attempts=3, min_wait_seconds=0.1, max_wait_seconds=10, multiplier=1)
NO_RETRY = RetryConfig(max_attempts=1)


def gap_url(run: str, name: str) -> str:
    return f"{MOCK_BASE}/gaps/{run}/{name}"


def gap_destination(run: str, name: str, **params: Any) -> Any:
    """An api_destination that POSTs ``{"records": [...]}`` to /gaps/{run}/{name}."""
    retry = params.pop("retry_config", FAST_RETRY)
    return api_destination(
        url=gap_url(run, name),
        params={k: str(v) for k, v in params.items()} or None,
        retry_config=retry,
        per_record=True,
    )


# ---------------------------------------------------------------------------
# Sources: SQL bind parameters, | ident, trailing ';', secret(), redaction
# ---------------------------------------------------------------------------


class SqlParams(RuntimeParams, total=False):
    run: str
    status: str
    table: Annotated[NotRequired[str], Param(default="test_events")]
    semicolon: Annotated[NotRequired[bool], Param(default=False)]
    use_secret: Annotated[NotRequired[bool], Param(default=False)]


class GapsSqlPipeline(AbstractPipeline[SqlParams]):
    """``status`` is bound (an injection attempt matches nothing); ``table`` goes
    through ``| ident`` (a non-identifier fails the run at planning)."""

    name = "e2e_gaps_sql"

    def define_source(self, runtime_params: SqlParams) -> Any:
        query = (
            "SELECT id, status FROM {{ table | ident }} "
            "WHERE status = {{ status }} ORDER BY id LIMIT 5"
        )
        if runtime_params.get("semicolon"):
            query += ";"
        url: Any = secret("SQL_CONNECTION_URL") if runtime_params.get("use_secret") else SQL_URL
        return sql_source(connection_url=url, query=query)

    def define_transformations(self, records: List[Any], runtime_params: SqlParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: SqlParams) -> Any:
        return gap_destination(runtime_params.get("run", "x"), "sql")


# ---------------------------------------------------------------------------
# Transformations: options, should_apply_transformation, on_error="skip",
# the one-Record-per-record contract
# ---------------------------------------------------------------------------


@transformation("gaps_label")
def gaps_label(records: List[Any], runtime_params: Any, *, label: str) -> List[Any]:
    return [{**r, "label": label} for r in records]


@transformation("gaps_declined")
def gaps_declined(records: List[Any], runtime_params: Any) -> List[Any]:
    return [{**r, "declined_step_ran": True} for r in records]


@record_transformation("gaps_skip_odd", on_error="skip")
async def gaps_skip_odd(record: Dict[str, Any], runtime_params: Any) -> Dict[str, Any]:
    if record["id"] % 2:
        raise ValueError("odd ids are rejected")
    return record


@record_transformation("gaps_returns_tuple")
async def gaps_returns_tuple(record: Dict[str, Any], runtime_params: Any) -> Any:
    return (record, record)  # breaks the contract: a record, a list of records, or None


class TransformParams(RuntimeParams, total=False):
    run: str
    scenario: Annotated[NotRequired[str], Param(default="ok")]


class GapsTransformPipeline(AbstractPipeline[TransformParams]):
    name = "e2e_gaps_transform"

    def define_jobs(self, runtime_params: TransformParams) -> Any:
        yield job([{"id": i} for i in range(1, 7)])

    def define_transformations(
        self, records: List[Any], runtime_params: TransformParams
    ) -> List[Any]:
        steps: List[Any] = [gaps_label(label="opt"), gaps_declined(), gaps_skip_odd()]
        if runtime_params.get("scenario") == "contract":
            steps.append(gaps_returns_tuple())
        return steps

    def should_apply_transformation(
        self, transformation: Any, runtime_params: TransformParams, records: List[Any]
    ) -> bool:
        return transformation.name != "gaps_declined"

    def define_destination(self, records: List[Any], runtime_params: TransformParams) -> Any:
        return gap_destination(runtime_params.get("run", "x"), "out")


# ---------------------------------------------------------------------------
# Destinations: fan_out (+ optional), multicast @record_destination, a permanent
# 4xx is not retried, Retry-After is honoured
# ---------------------------------------------------------------------------


@record_destination("gaps_multicast")
def gaps_multicast(record: Dict[str, Any], runtime_params: Any) -> Any:
    run = runtime_params.get("run", "x")
    if record["id"] % 2 == 0:
        return [gap_destination(run, "main"), gap_destination(run, "even")]
    return gap_destination(run, "main")


class DestParams(RuntimeParams, total=False):
    run: str
    scenario: str


class GapsDestPipeline(AbstractPipeline[DestParams]):
    name = "e2e_gaps_dest"

    def define_jobs(self, runtime_params: DestParams) -> Any:
        # One record where a test counts requests: a job-level retry re-sends them all.
        single = runtime_params.get("scenario") in ("permanent", "retry_after")
        yield job([{"id": 1}] if single else [{"id": i} for i in range(1, 5)])

    def define_transformations(self, records: List[Any], runtime_params: DestParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: DestParams) -> Any:
        run, mode = runtime_params.get("run", "x"), runtime_params.get("scenario")
        if mode == "fanout":
            broken = gap_destination(run, "optional", status=500, retry_config=NO_RETRY)
            return fan_out(gap_destination(run, "main"), broken, optional=[broken])
        if mode == "fanout_required":
            broken = gap_destination(run, "required", status=500, retry_config=NO_RETRY)
            return fan_out(gap_destination(run, "main"), broken)
        if mode == "list":
            return [gap_destination(run, "main"), gap_destination(run, "copy")]
        if mode == "multicast":
            return gaps_multicast()
        if mode == "permanent":
            return gap_destination(run, "permanent", status=404)
        if mode == "retry_after":
            return gap_destination(run, "retry_after", fail_first=1, retry_after="2")
        raise ValueError(f"unknown mode {mode!r}")


# ---------------------------------------------------------------------------
# Job retries (JobRetryConfig)
# ---------------------------------------------------------------------------


class RetryParams(RuntimeParams, total=False):
    run: str
    status: Annotated[NotRequired[int], Param(default=200)]
    fail_first: Annotated[NotRequired[int], Param(default=0)]


class GapsRetryPipeline(AbstractPipeline[RetryParams]):
    """One job whose destination does not retry by itself, so each failure is a job
    failure that ``job_retry`` decides about."""

    name = "e2e_gaps_retry"
    job_retry = JobRetryConfig(max_retries=2, backoff_seconds=1, multiplier=1)

    def define_jobs(self, runtime_params: RetryParams) -> Any:
        yield job([{"id": 1}])

    def define_transformations(self, records: List[Any], runtime_params: RetryParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: RetryParams) -> Any:
        return gap_destination(
            runtime_params.get("run", "x"),
            "retry",
            status=runtime_params.get("status", 200),
            fail_first=runtime_params.get("fail_first", 0),
            retry_config=NO_RETRY,
        )


# ---------------------------------------------------------------------------
# Lifecycle hooks, run_pipeline chaining, define_rate_limit
# ---------------------------------------------------------------------------


class HookParams(RuntimeParams, total=False):
    run: str
    fail: Annotated[NotRequired[bool], Param(default=False)]
    chain: Annotated[NotRequired[bool], Param(default=False)]
    rpm: Annotated[NotRequired[float], Param(default=0.0)]


def _post_summary(run: str, name: str, summary: ExecutionSummary) -> None:
    httpx.post(gap_url(run, name), json=summary.to_dict(), timeout=10).raise_for_status()


class GapsHooksPipeline(AbstractPipeline[HookParams]):
    name = "e2e_gaps_hooks"

    def define_jobs(self, runtime_params: HookParams) -> Any:
        yield job([{"id": 1}, {"id": 2}])

    def define_rate_limit(self, runtime_params: HookParams) -> Any:
        return runtime_params.get("rpm") or None

    def define_transformations(self, records: List[Any], runtime_params: HookParams) -> List[Any]:
        if runtime_params.get("fail"):
            raise RuntimeError("asked to fail")
        return []

    def define_destination(self, records: List[Any], runtime_params: HookParams) -> Any:
        return gap_destination(runtime_params.get("run", "x"), "data")

    def on_execution_complete(self, summary: ExecutionSummary, runtime_params: HookParams) -> None:
        run = runtime_params.get("run", "x")
        _post_summary(run, "hook_complete", summary)
        if runtime_params.get("chain"):
            run_pipeline(self.name, {"run": f"{run}-child"})

    def on_execution_failed(self, summary: ExecutionSummary, runtime_params: HookParams) -> None:
        _post_summary(runtime_params.get("run", "x"), "hook_failed", summary)
