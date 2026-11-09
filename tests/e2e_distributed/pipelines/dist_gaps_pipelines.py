"""Distributed E2E pipelines for the framework-gaps features (PRs #105–#110).

Same conventions as ``dist_pipelines.py``: ``run`` is the sink channel, ``sink_url``
the sink as the workers see it, records are generated (``id = 1..total``) and every
delivery goes to the sink's ledger, so a test proves what arrived over Kafka and real
workers. Copied into a project with ``sync_pipelines.sh``.
"""

from typing import Any, Dict, Iterator, List, Optional

import httpx
from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseSource,
    ExecutionSummary,
    JobRetryConfig,
    Param,
    RetryConfig,
    RuntimeParams,
    api_destination,
    chunk,
    fan_out,
    record_destination,
    record_transformation,
    run_pipeline,
    secret,
    transformation,
)

SINK = "http://e2e-sink:8080"
NO_RETRY = RetryConfig(max_attempts=1)


class GapsParams(RuntimeParams, total=False):
    run: Required[str]
    sink_url: Annotated[NotRequired[str], Param("Sink base URL as the workers see it", default=SINK)]
    total: Annotated[NotRequired[int], Param("Records", default=20)]
    per_job: Annotated[NotRequired[int], Param("Records per job", default=5)]
    mode: Annotated[NotRequired[str], Param("What to exercise", default="")]
    rpm: Annotated[NotRequired[float], Param("define_rate_limit result", default=0.0)]
    chain: Annotated[NotRequired[bool], Param("Start a child run from the hook", default=False)]
    fail: Annotated[NotRequired[bool], Param("Fail every job", default=False)]


def _sink(params: Any) -> str:
    return str(params.get("sink_url", SINK))


def _jobs(params: Any) -> List[List[Dict[str, Any]]]:
    records = [{"id": i} for i in range(1, int(params.get("total", 20)) + 1)]
    return chunk(records, size=int(params.get("per_job", 5)))


def _ingest(params: Any, channel: str, records: List[Any], retry: Optional[RetryConfig] = None, **extra: Any) -> Any:
    return api_destination(
        url=f"{_sink(params)}/ingest/{channel}",
        body={"records": [{"id": r["id"]} for r in records], **extra},
        retry_config=retry,
    )


def _per_record(params: Any, channel: str) -> Any:
    return api_destination(url=f"{_sink(params)}/record/{channel}/{{id}}", method="PUT", per_record=True)


# --------------------------------------------------------------------------------
# A user-defined source, rebuilt on the worker (#2), carrying a secret() (#5)
# --------------------------------------------------------------------------------


class DistGeneratedSource(BaseSource):
    """Generates ids ``first..last``; ``token`` is a secret() the worker resolves."""

    secret_fields = ("token",)

    def __init__(self, first: int, last: int, token: Any = None):
        super().__init__({"first": first, "last": last, "token": token})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["DistGeneratedSource"]:
        step = int(runtime_params.get("per_job", 5))
        for start in range(self.config["first"], self.config["last"] + 1, step):
            end = min(start + step - 1, self.config["last"])
            yield DistGeneratedSource(start, end, self.config["token"])

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        token = self.config["token"]
        resolved = isinstance(token, str) and bool(token) and not token.startswith("{")
        return [
            {"id": i, "token_resolved": resolved}
            for i in range(self.config["first"], self.config["last"] + 1)
        ]

    def health_check(self) -> bool:
        return True


class DistGapsSource(AbstractPipeline[GapsParams]):
    name = "dist_gaps_source"

    def define_source(self, runtime_params: GapsParams) -> Any:
        # HOSTNAME is set in every container: the manager and each worker resolve it.
        return DistGeneratedSource(1, int(runtime_params.get("total", 20)), secret("HOSTNAME"))

    def define_transformations(self, records: List[Any], runtime_params: GapsParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: GapsParams) -> Any:
        return _ingest(
            runtime_params,
            runtime_params["run"],
            records,
            token_resolved=all(r["token_resolved"] for r in records),
        )


# --------------------------------------------------------------------------------
# Transformations: options (#10), should_apply_transformation (#3),
# on_error="skip" (#11), the one-record contract (#14)
# --------------------------------------------------------------------------------


@transformation("dist_gaps_label")
def dist_gaps_label(records: List[Any], runtime_params: Any, *, label: str) -> List[Any]:
    return [{**r, "label": label} for r in records]


@transformation("dist_gaps_declined")
def dist_gaps_declined(records: List[Any], runtime_params: Any) -> List[Any]:
    return [{**r, "declined_ran": True} for r in records]


@record_transformation("dist_gaps_skip_odd", on_error="skip")
async def dist_gaps_skip_odd(record: Dict[str, Any], runtime_params: Any) -> Dict[str, Any]:
    if record["id"] % 2:
        raise ValueError("odd")
    return record


@record_transformation("dist_gaps_returns_list")
async def dist_gaps_returns_list(record: Dict[str, Any], runtime_params: Any) -> Any:
    return [record]


class DistGapsTransform(AbstractPipeline[GapsParams]):
    name = "dist_gaps_transform"

    def define_jobs(self, runtime_params: GapsParams) -> Any:
        return _jobs(runtime_params)

    def define_transformations(self, records: List[Any], runtime_params: GapsParams) -> List[Any]:
        steps: List[Any] = [dist_gaps_label(label="dist"), dist_gaps_declined(), dist_gaps_skip_odd()]
        if runtime_params.get("mode") == "contract":
            steps.append(dist_gaps_returns_list())
        return steps

    def should_apply_transformation(self, transformation: Any, runtime_params: GapsParams, records: List[Any]) -> bool:
        return transformation.name != "dist_gaps_declined"

    def define_destination(self, records: List[Any], runtime_params: GapsParams) -> Any:
        return _ingest(
            runtime_params,
            runtime_params["run"],
            records,
            labels=sorted({r["label"] for r in records}),
            declined_ran=any(r.get("declined_ran") for r in records),
        )


# --------------------------------------------------------------------------------
# Destinations: fan_out + optional, a list, per-record multicast (#17)
# --------------------------------------------------------------------------------


@record_destination("dist_gaps_multicast")
def dist_gaps_multicast(record: Dict[str, Any], runtime_params: Any) -> Any:
    run = runtime_params["run"]
    if record["id"] % 2 == 0:
        return [_per_record(runtime_params, run), _per_record(runtime_params, f"{run}-even")]
    return _per_record(runtime_params, run)


class DistGapsDest(AbstractPipeline[GapsParams]):
    name = "dist_gaps_dest"

    def define_jobs(self, runtime_params: GapsParams) -> Any:
        return _jobs(runtime_params)

    def define_transformations(self, records: List[Any], runtime_params: GapsParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: GapsParams) -> Any:
        run, mode = runtime_params["run"], runtime_params.get("mode")
        if mode == "fanout":
            optional = _ingest(runtime_params, f"{run}-opt", records, retry=NO_RETRY)
            return fan_out(_ingest(runtime_params, run, records), optional, optional=[optional])
        if mode == "list":
            return [_ingest(runtime_params, run, records), _ingest(runtime_params, f"{run}-copy", records)]
        if mode == "multicast":
            return dist_gaps_multicast()
        raise ValueError(f"unknown mode {mode!r}")


# --------------------------------------------------------------------------------
# Job retries (#23) and a permanent 4xx (#8)
# --------------------------------------------------------------------------------


class DistGapsRetry(AbstractPipeline[GapsParams]):
    """The destination does not retry on its own, so each failed request fails its job
    and ``job_retry`` re-runs it on a worker."""

    name = "dist_gaps_retry"
    job_retry = JobRetryConfig(max_retries=3, backoff_seconds=1, multiplier=1)

    def define_jobs(self, runtime_params: GapsParams) -> Any:
        return _jobs(runtime_params)

    def define_transformations(self, records: List[Any], runtime_params: GapsParams) -> List[Any]:
        return []

    def define_destination(self, records: List[Any], runtime_params: GapsParams) -> Any:
        return _ingest(runtime_params, runtime_params["run"], records, retry=NO_RETRY)


# --------------------------------------------------------------------------------
# Lifecycle hooks, run_pipeline chaining (#25), define_rate_limit (#3)
# --------------------------------------------------------------------------------


def _hook_post(params: Any, outcome: str, summary: ExecutionSummary) -> None:
    httpx.post(
        f"{_sink(params)}/ingest/{params['run']}-hook",
        json={"records": [{"id": outcome}], "summary": summary.to_dict()},
        timeout=10,
    ).raise_for_status()


class DistGapsHooks(AbstractPipeline[GapsParams]):
    name = "dist_gaps_hooks"

    def define_jobs(self, runtime_params: GapsParams) -> Any:
        return _jobs(runtime_params)

    def define_rate_limit(self, runtime_params: GapsParams) -> Any:
        return runtime_params.get("rpm") or None

    def define_transformations(self, records: List[Any], runtime_params: GapsParams) -> List[Any]:
        if runtime_params.get("fail"):
            raise RuntimeError("asked to fail")
        return []

    def define_destination(self, records: List[Any], runtime_params: GapsParams) -> Any:
        return _ingest(runtime_params, runtime_params["run"], records)

    def on_execution_complete(self, summary: ExecutionSummary, runtime_params: GapsParams) -> None:
        _hook_post(runtime_params, "complete", summary)
        if runtime_params.get("chain"):
            run_pipeline(
                self.name,
                {"run": f"{runtime_params['run']}-child", "sink_url": _sink(runtime_params), "total": 3},
            )

    def on_execution_failed(self, summary: ExecutionSummary, runtime_params: GapsParams) -> None:
        _hook_post(runtime_params, "failed", summary)
