"""Async transformation E2E pipeline: a per-record lookup, an async batch step, a sync step.

One job of ``count`` records. ``EnrichFromSlowUsers`` awaits the mock API's
``/slow-users`` once per record, ``concurrency`` at a time; the mock counts the
calls in flight, so the test can check the cap held and that the records really
ran concurrently. ``e2e_async_label`` is a whole-batch ``async def``;
``e2e_sync_stamp`` is a plain sync step in the same chain.

Behaviour switches: ``drop_id`` (the API answers 404 → that record is dropped),
``fail_id`` (500 → the job fails naming the record), ``skip`` (the async step
raises SkipJob).
"""

import os
from typing import Any, Dict, List, Optional

import httpx
from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    BaseSource,
    Param,
    Record,
    Records,
    RecordTransformation,
    RuntimeParams,
    SkipJob,
    Transformations,
    transformation,
)
from tests.e2e.test_pipelines.destinations import e2e_http
from tests.e2e.test_pipelines.sources import e2e_mock

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class AsyncTransformParams(RuntimeParams, total=False):
    marker: Required[str]
    count: Annotated[NotRequired[int], Param("Records in the job", default=40)]
    concurrency: Annotated[NotRequired[int], Param("Lookups in flight at once", default=8)]
    delay_ms: Annotated[NotRequired[int], Param("Mock API latency per lookup", default=200)]
    drop_id: Annotated[NotRequired[int], Param("Id the API answers 404 for", default=0)]
    fail_id: Annotated[NotRequired[int], Param("Id the API answers 500 for", default=0)]
    skip: Annotated[NotRequired[bool], Param("The async step raises SkipJob", default=False)]


class EnrichFromSlowUsers(RecordTransformation):
    """Per-record lookup. The class form: concurrency comes from a param, and one
    client serves every record of the step."""

    name = "e2e_enrich_slow_user"

    def __init__(self, concurrency: int):
        self.concurrency = concurrency
        self.timeout = 30.0
        self.client: Optional[httpx.AsyncClient] = None

    async def setup(self, runtime_params: Dict[str, Any]) -> None:
        self.client = httpx.AsyncClient(base_url=MOCK_API_URL, timeout=30.0)

    async def teardown(self) -> None:
        if self.client is not None:
            await self.client.aclose()

    async def apply_record(self, record: Any, runtime_params: Dict[str, Any]) -> Optional[Record]:
        data: Dict[str, Any] = record["data"]
        user_id = int(data["id"])
        status = 200
        if user_id == runtime_params.get("drop_id"):
            status = 404
        elif user_id == runtime_params.get("fail_id"):
            status = 500
        assert self.client is not None  # opened by setup()
        resp = await self.client.get(
            f"/slow-users/{runtime_params['marker']}/{user_id}",
            params={"delay_ms": runtime_params.get("delay_ms", 200), "status": status},
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return {"data": {**data, "user": resp.json()["name"]}}


@transformation("e2e_async_label")
async def e2e_async_label(records: Records, runtime_params: AsyncTransformParams) -> Records:
    if runtime_params.get("skip"):
        raise SkipJob("async step asked to skip")
    return [{"data": {**r["data"], "label": "async"}} for r in records]


@transformation("e2e_sync_stamp")
def e2e_sync_stamp(records: Records, runtime_params: AsyncTransformParams) -> Records:
    return [{"data": {**r["data"], "stamp": "sync"}} for r in records]


class E2EAsyncTransformPipeline(AbstractPipeline[AsyncTransformParams]):
    name = "e2e_async_transform"
    description = "E2E: per-record async lookup + async batch step + sync step"

    def define_source(self, runtime_params: AsyncTransformParams) -> BaseSource:
        count = runtime_params.get("count", 40)
        data: List[Any] = [
            {"id": i, "marker": runtime_params["marker"]} for i in range(1, count + 1)
        ]
        return e2e_mock(data=data, batch_size=count)

    def define_transformations(
        self, records: Records, runtime_params: AsyncTransformParams
    ) -> Transformations:
        return [
            EnrichFromSlowUsers(concurrency=runtime_params.get("concurrency", 8)),
            e2e_async_label(),
            e2e_sync_stamp(),
        ]

    def define_destination(
        self, records: Records, runtime_params: AsyncTransformParams
    ) -> BaseDestination:
        return e2e_http(body={"records": records})
