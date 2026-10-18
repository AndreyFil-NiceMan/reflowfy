"""Pipelines for the distributed E2E suite (``tests/e2e_distributed``).

They generate their own records (no external data needed) and deliver to the sink over
``api_destination``, so a test can check from outside exactly what arrived. Copy this
file into a project's ``src/pipelines/`` and build it into the images
(``tests/e2e_distributed/sync_pipelines.sh``) — the manager and workers must both import it.

AbstractPipeline
    dist_abstract         ``total`` records in jobs of ``per_job``; behaviour switches below
    dist_abstract_es      the same delivery from an Elasticsearch index (needs ES + a seeded index)
IdBasedPipeline
    dist_ids_b1 / _b7 / _b100   IDs are the records; ids_batch_size 1 / 7 / 100
    dist_ids_fanout             every input ID fans out into three jobs via define_jobs
    dist_ids_api                per-ID GET against the sink's mock users API
    dist_ids_api_batch          one POST per ID batch against the mock users API

Behaviour switches (runtime params): ``skip_jobs`` / ``fail_jobs`` (CSV of job indexes),
``fail_while_flag`` (jobs raise while the sink flag of the run equals this value — the way
a test makes a failure that it later "fixes" before retry-failed), ``drop_even``,
``expand``, ``slow_s``, ``pad_kb``, ``volatile``.
"""

import time
from typing import Any, Dict, List, Set

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    BaseSource,
    IdBasedPipeline,
    Param,
    Records,
    RuntimeParams,
    SkipJob,
    Transformations,
    chunk,
    job,
    transformation,
)
from reflowfy.destinations.api import api_destination
from reflowfy.sources.api import id_based_api_source
from reflowfy.sources.elastic import elastic_source

SINK = "http://e2e-sink:8080"


class DistParams(RuntimeParams, total=False):
    run: Required[str]
    sink_url: Annotated[NotRequired[str], Param("Sink base URL as the workers see it", default=SINK)]
    total: Annotated[NotRequired[int], Param("Records to generate", default=1000)]
    per_job: Annotated[NotRequired[int], Param("Records per job", default=100)]
    skip_jobs: Annotated[NotRequired[str], Param("CSV of job indexes that raise SkipJob", default="")]
    fail_jobs: Annotated[NotRequired[str], Param("CSV of job indexes that always fail", default="")]
    fail_while_flag: Annotated[
        NotRequired[str], Param("Jobs fail while the run's sink flag equals this", default="")
    ]
    drop_even: Annotated[NotRequired[bool], Param("Filter out even ids", default=False)]
    expand: Annotated[NotRequired[bool], Param("Turn each record into two", default=False)]
    slow_s: Annotated[NotRequired[float], Param("Sleep per job before delivery", default=0.0)]
    pad_kb: Annotated[NotRequired[int], Param("Padding per record, KB", default=0)]
    volatile: Annotated[NotRequired[bool], Param("Put a timestamp in the delivered body", default=False)]
    dest_tag: Annotated[
        NotRequired[str], Param("Adds ?tag=… to the destination URL: same records, other destination", default="")
    ]
    stable_identity: Annotated[
        NotRequired[bool],
        Param("Declare the destination's dedup identity (so a volatile body does not matter)", default=False),
    ]
    plan_delay_s: Annotated[
        NotRequired[float], Param("Seconds the planner blocks per job (simulates a slow source)", default=0.0)
    ]
    job_index: Annotated[int, Param(internal=True)]


class DistIdParams(RuntimeParams, total=False):
    run: Required[str]
    sink_url: Annotated[NotRequired[str], Param("Sink base URL as the workers see it", default=SINK)]
    fail_ids: Annotated[NotRequired[str], Param("CSV of ids whose job always fails", default="")]
    fail_while_flag: Annotated[
        NotRequired[str], Param("Jobs fail while the run's sink flag equals this", default="")
    ]
    slow_s: Annotated[NotRequired[float], Param("Sleep per job before delivery", default=0.0)]


class EsParams(RuntimeParams, total=False):
    run: Required[str]
    sink_url: Annotated[NotRequired[str], Param("Sink base URL as the workers see it", default=SINK)]
    es_url: Annotated[NotRequired[str], Param("Elasticsearch URL as the workers see it", default="http://es:9200")]
    min_seq: Annotated[NotRequired[int], Param("Lowest seq", default=1)]
    max_seq: Annotated[NotRequired[int], Param("Highest seq", default=10000)]
    docs_per_job: Annotated[NotRequired[int], Param("Docs per job", default=500)]
    window_mode: Annotated[NotRequired[str], Param("snapshot | range", default="snapshot")]


def _csv_ints(value: Any) -> Set[int]:
    return {int(x) for x in str(value or "").split(",") if x.strip()}


def _fail_while_flag(params: Dict[str, Any]) -> None:
    """Raise while the sink's flag for this run equals ``fail_while_flag``."""
    wanted = params.get("fail_while_flag")
    if not wanted:
        return
    import httpx

    sink = params.get("sink_url", SINK)
    current = httpx.get(f"{sink}/flag/{params['run']}", timeout=10).json().get("value", "")
    if current == wanted:
        raise RuntimeError(f"injected failure: sink flag is {current!r}")


# --------------------------------------------------------------------------------
# AbstractPipeline
# --------------------------------------------------------------------------------


@transformation("dist_behaviors")
def dist_behaviors(records: Records, runtime_params: DistParams) -> Records:
    index = runtime_params.get("job_index")
    if index is not None and index in _csv_ints(runtime_params.get("skip_jobs")):
        raise SkipJob(f"skip job {index}")
    if index is not None and index in _csv_ints(runtime_params.get("fail_jobs")):
        raise RuntimeError(f"injected failure in job {index}")
    _fail_while_flag(dict(runtime_params))
    if runtime_params.get("slow_s"):
        time.sleep(float(runtime_params["slow_s"]))
    out = list(records)
    if runtime_params.get("drop_even"):
        out = [r for r in out if r["id"] % 2]
    if runtime_params.get("expand"):
        out = [{**r, "id": r["id"] * 10 + n} for r in out for n in (0, 1)]
    return out


class DistAbstract(AbstractPipeline[DistParams]):
    name = "dist_abstract"
    description = "Distributed E2E: generated records -> sink"
    rate_limit = 6000

    def define_jobs(self, runtime_params: DistParams):
        pad = "x" * (runtime_params.get("pad_kb", 0) * 1024)
        records = [
            {"id": i, "run": runtime_params["run"], **({"pad": pad} if pad else {})}
            for i in range(1, runtime_params.get("total", 1000) + 1)
        ]
        for index, part in enumerate(chunk(records, runtime_params.get("per_job", 100))):
            if runtime_params.get("plan_delay_s"):
                time.sleep(float(runtime_params["plan_delay_s"]))  # a slow planner
            yield job(part, job_index=index)

    def define_transformations(self, records: Records, runtime_params: DistParams) -> Transformations:
        return [dist_behaviors()]

    def define_destination(self, records: Records, runtime_params: DistParams) -> BaseDestination:
        # ids only, unless padding was asked for (then the whole record travels)
        sent: List[Any] = records if runtime_params.get("pad_kb") else [{"id": r["id"]} for r in records]
        body: Dict[str, Any] = {"job_index": runtime_params.get("job_index"), "records": sent}
        if runtime_params.get("volatile"):
            body["ts"] = time.time()
        sink = runtime_params.get("sink_url", SINK)
        tag = runtime_params.get("dest_tag")
        url = f"{sink}/ingest/{runtime_params['run']}"
        return api_destination(
            url=url,
            body=body,
            params={"tag": tag} if tag else None,
            dedup_identity={"url": url, "tag": tag} if runtime_params.get("stable_identity") else None,
        )


class DistAbstractEs(AbstractPipeline[EsParams]):
    name = "dist_abstract_es"
    description = "Distributed E2E: Elasticsearch docs (seq) -> sink"
    rate_limit = 6000

    def define_source(self, runtime_params: EsParams) -> BaseSource:
        return elastic_source(
            url=runtime_params.get("es_url", "http://es:9200"),
            index="docs",
            base_query={
                "query": {
                    "range": {
                        "seq": {
                            "gte": runtime_params.get("min_seq", 1),
                            "lte": runtime_params.get("max_seq", 10000),
                        }
                    }
                },
                "sort": [{"seq": "asc"}],
            },
            size=500,
            docs_per_job=runtime_params.get("docs_per_job", 500),
            window_mode=runtime_params.get("window_mode", "snapshot"),
        )

    def define_transformations(self, records: Records, runtime_params: EsParams) -> Transformations:
        return []

    def define_destination(self, records: Records, runtime_params: EsParams) -> BaseDestination:
        sink = runtime_params.get("sink_url", SINK)
        body = {"records": [{"id": r["data"]["seq"]} for r in records]}
        return api_destination(url=f"{sink}/ingest/{runtime_params['run']}", body=body)


# --------------------------------------------------------------------------------
# IdBasedPipeline
# --------------------------------------------------------------------------------


@transformation("dist_id_behaviors")
def dist_id_behaviors(records: Records, runtime_params: DistIdParams) -> Records:
    current: List[Any] = runtime_params.get("current_ids") or []  # type: ignore[assignment]
    fail_ids = {x for x in str(runtime_params.get("fail_ids") or "").split(",") if x.strip()}
    if fail_ids & {str(i) for i in current}:
        raise RuntimeError(f"injected failure for ids {sorted(fail_ids & {str(i) for i in current})}")
    _fail_while_flag(dict(runtime_params))
    if runtime_params.get("slow_s"):
        time.sleep(float(runtime_params["slow_s"]))
    return records


def _ids_destination(records: Records, params: DistIdParams, key: str = "id") -> BaseDestination:
    """Deliver records plus what the job knew about itself (for the visibility tests)."""
    current: Any = params.get("current_ids")
    ids: List[Any] = []
    for record in records:
        if isinstance(record, dict):
            data: Any = record.get("data", record)
            ids.append({"id": data.get(key) if isinstance(data, dict) else data})
        else:
            ids.append({"id": record})
    body = {
        "records": ids,
        "current_ids": current,
        "current_id": params.get("current_id"),
        "has_input_ids": "input_ids" in params,
    }
    sink = params.get("sink_url", SINK)
    return api_destination(url=f"{sink}/ingest/{params['run']}", body=body)


class DistIdsB1(IdBasedPipeline[DistIdParams]):
    name = "dist_ids_b1"
    rate_limit = 6000
    ids_batch_size = 1

    def define_source(self, runtime_params: DistIdParams):
        return runtime_params["current_ids"]  # type: ignore[typeddict-item]

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return [dist_id_behaviors()]

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        return _ids_destination(records, runtime_params)


class DistIdsB7(IdBasedPipeline[DistIdParams]):
    name = "dist_ids_b7"
    rate_limit = 6000
    ids_batch_size = 7

    def define_source(self, runtime_params: DistIdParams):
        return runtime_params["current_ids"]  # type: ignore[typeddict-item]

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return [dist_id_behaviors()]

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        return _ids_destination(records, runtime_params)


class DistIdsB100(IdBasedPipeline[DistIdParams]):
    name = "dist_ids_b100"
    rate_limit = 6000
    ids_batch_size = 100

    def define_source(self, runtime_params: DistIdParams):
        return runtime_params["current_ids"]  # type: ignore[typeddict-item]

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return [dist_id_behaviors()]

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        return _ids_destination(records, runtime_params)


class DistIdsFanout(IdBasedPipeline[DistIdParams]):
    """Each input ID becomes three jobs (children ``<id>-0..2``)."""

    name = "dist_ids_fanout"
    rate_limit = 6000

    def define_jobs(self, runtime_params: DistIdParams):
        for parent in runtime_params.get("input_ids", []):  # type: ignore[typeddict-item]
            for child in range(3):
                yield job([{"id": f"{parent}-{child}", "parent": parent}], current_id=parent)

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return []

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        sink = runtime_params.get("sink_url", SINK)
        body = {
            "records": [{"id": r["id"]} for r in records],
            "current_id": runtime_params.get("current_id"),
        }
        return api_destination(url=f"{sink}/ingest/{runtime_params['run']}", body=body)


class DistIdsApi(IdBasedPipeline[DistIdParams]):
    """Per-ID ``GET /users/{id}`` against the sink's mock API (5 IDs per job)."""

    name = "dist_ids_api"
    rate_limit = 6000
    ids_batch_size = 5

    def define_source(self, runtime_params: DistIdParams) -> BaseSource:
        sink = runtime_params.get("sink_url", SINK)
        return id_based_api_source(
            base_url=f"{sink}/api/{runtime_params['run']}",
            endpoint_template="/users/{id}",
            ids=runtime_params["current_ids"],  # type: ignore[typeddict-item]
        )

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return []

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        return _ids_destination(records, runtime_params)


class DistIdsApiBatch(IdBasedPipeline[DistIdParams]):
    """One ``POST /users/batch`` per ID batch (batch mode of the API source)."""

    name = "dist_ids_api_batch"
    rate_limit = 6000
    ids_batch_size = 5

    def define_source(self, runtime_params: DistIdParams) -> BaseSource:
        sink = runtime_params.get("sink_url", SINK)
        return id_based_api_source(
            base_url=f"{sink}/api/{runtime_params['run']}",
            endpoint_template="/users/batch",
            method="POST",
            body={"ids": runtime_params["current_ids"]},  # type: ignore[typeddict-item]
            response_key="users",
        )

    def define_transformations(self, records: Records, runtime_params: DistIdParams) -> Transformations:
        return []

    def define_destination(self, records: Records, runtime_params: DistIdParams) -> BaseDestination:
        return _ids_destination(records, runtime_params)
