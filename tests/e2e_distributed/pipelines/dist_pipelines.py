"""Pipelines for the distributed E2E suite (``tests/e2e_distributed``).

They generate their own records (no external data needed) and deliver to the sink over
``api_destination``, so a test can check from outside exactly what arrived. Copy this
file into a project's ``src/pipelines/`` and build it into the images
(``tests/e2e_distributed/sync_pipelines.sh``) — the manager and workers must both import it.

AbstractPipeline
    dist_abstract         ``total`` records in jobs of ``per_job``; behaviour switches below
    dist_abstract_es      the same delivery from an Elasticsearch index (needs ES + a seeded index)
    dist_async            dist_abstract plus a per-record async lookup against the mock users API
    dist_per_record       dist_abstract delivered one PUT per record (api_destination
                          per_record=True), ``dest_concurrency`` requests in flight per job
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
from typing import Any, Dict, Iterator, List, Optional, Set

import httpx

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    BaseSource,
    IdBasedPipeline,
    Param,
    Record,
    Records,
    RecordTransformation,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    Transformations,
    chunk,
    job,
    transformation,
)
from reflowfy.destinations.api import api_destination
from reflowfy.destinations.base import RetryConfig
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


def _generated_jobs(runtime_params: DistParams) -> Iterator[Any]:
    """``total`` generated records in jobs of ``per_job``, each tagged with its job_index."""
    pad = "x" * (runtime_params.get("pad_kb", 0) * 1024)
    records = [
        {"id": i, "run": runtime_params["run"], **({"pad": pad} if pad else {})}
        for i in range(1, runtime_params.get("total", 1000) + 1)
    ]
    for index, part in enumerate(chunk(records, runtime_params.get("per_job", 100))):
        if runtime_params.get("plan_delay_s"):
            time.sleep(float(runtime_params["plan_delay_s"]))  # a slow planner
        yield job(part, job_index=index)


class DistAbstract(AbstractPipeline[DistParams]):
    name = "dist_abstract"
    description = "Distributed E2E: generated records -> sink"
    rate_limit = 6000

    def define_jobs(self, runtime_params: DistParams):
        return _generated_jobs(runtime_params)

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


class DistAsyncParams(DistParams, total=False):
    concurrency: Annotated[NotRequired[int], Param("Lookups in flight at once, per job", default=8)]
    retries: Annotated[NotRequired[int], Param("Retries per lookup on a 5xx (the author's own)", default=0)]
    skip_ids: Annotated[NotRequired[str], Param("CSV of ids dropped with SkipRecord", default="")]


class DistLookupUser(RecordTransformation):
    """Per-record GET of the mock users API, ``concurrency`` at a time, one client per job.

    Each call carries ``?job=<job_index>`` so the sink can report peak concurrency per job.
    A 404 drops the record; a 5xx is retried ``retries`` times, then fails the job;
    an id in ``skip_ids`` is dropped with SkipRecord after its lookup.
    """

    name = "dist_lookup_user"

    def __init__(self, concurrency: int, retries: int):
        self.concurrency = concurrency
        self.retries = retries
        self.timeout = 120.0
        self.client: Optional[httpx.AsyncClient] = None

    async def setup(self, runtime_params: Dict[str, Any]) -> None:
        self.client = httpx.AsyncClient(base_url=runtime_params.get("sink_url", SINK), timeout=60.0)

    async def teardown(self) -> None:
        if self.client is not None:
            await self.client.aclose()

    async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Optional[Record]:
        assert self.client is not None  # opened by setup()
        path = f"/api/{runtime_params['run']}/users/{record['id']}"
        query = {"job": runtime_params.get("job_index")}
        resp = await self.client.get(path, params=query)
        for _ in range(self.retries):
            if resp.status_code < 500:
                break
            resp = await self.client.get(path, params=query)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        if record["id"] in _csv_ints(runtime_params.get("skip_ids")):
            raise SkipRecord("skip requested")
        return {**record, "user": resp.json()["name"]}


class DistAsync(AbstractPipeline[DistAsyncParams]):
    name = "dist_async"
    description = "Distributed E2E: generated records -> per-record async lookup -> sink"
    rate_limit = 6000

    def define_jobs(self, runtime_params: DistAsyncParams):
        return _generated_jobs(runtime_params)

    def define_transformations(self, records: Records, runtime_params: DistAsyncParams) -> Transformations:
        return [
            dist_behaviors(),
            DistLookupUser(runtime_params.get("concurrency", 8), runtime_params.get("retries", 0)),
        ]

    def define_destination(self, records: Records, runtime_params: DistAsyncParams) -> BaseDestination:
        sink = runtime_params.get("sink_url", SINK)
        body: Dict[str, Any] = {
            "job_index": runtime_params.get("job_index"),
            "records": [{"id": r["id"]} for r in records],
            # the lookup's result rides beside the records, so a test can check it arrived
            "users": {str(r["id"]): r["user"] for r in records},
        }
        return api_destination(url=f"{sink}/ingest/{runtime_params['run']}", body=body)


class DistPerRecordParams(DistParams, total=False):
    dest_concurrency: Annotated[
        NotRequired[int], Param("Destination requests in flight at once, per job", default=1)
    ]
    use_key_field: Annotated[
        NotRequired[bool], Param("Send only record['payload'] (key_field)", default=False)
    ]


@transformation("dist_per_record_shape")
def dist_per_record_shape(records: Records, runtime_params: DistPerRecordParams) -> Records:
    """Ids only; with use_key_field nested under ``payload`` beside a field not to send."""
    if runtime_params.get("use_key_field"):
        return [{"id": r["id"], "payload": {"id": r["id"]}, "internal": "x"} for r in records]
    return [{"id": r["id"]} for r in records]


class DistPerRecord(AbstractPipeline[DistPerRecordParams]):
    """Every record in its own ``PUT <sink>/record/<run>/<id>?job=<job_index>``.

    The id in the URL comes from the record (the ``{id}`` placeholder); the sink checks it
    matches the body, and counts the requests of one job open at once.
    """

    name = "dist_per_record"
    description = "Distributed E2E: generated records -> one request per record -> sink"
    rate_limit = 6000

    def define_jobs(self, runtime_params: DistPerRecordParams):
        return _generated_jobs(runtime_params)

    def define_transformations(
        self, records: Records, runtime_params: DistPerRecordParams
    ) -> Transformations:
        return [dist_behaviors(), dist_per_record_shape()]

    def define_destination(
        self, records: Records, runtime_params: DistPerRecordParams
    ) -> BaseDestination:
        sink = runtime_params.get("sink_url", SINK)
        return api_destination(
            url=f"{sink}/record/{runtime_params['run']}/{{id}}",
            method="PUT",
            per_record=True,
            key_field="payload" if runtime_params.get("use_key_field") else None,
            concurrency=runtime_params.get("dest_concurrency", 1),
            params={"job": str(runtime_params.get("job_index"))},
            retry_config=RetryConfig(max_attempts=1),  # a failure fails the job at once
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
