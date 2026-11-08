"""
API destination in per-record mode.

Every record goes in its own ``PUT /items/<run>/<id>`` to the mock webhook server:
the record's ``id`` fills the URL placeholder, and the record (or only its ``payload``
with ``use_body_field``) is the body. A plain ``api_destination`` keeps up to 16
requests in flight per job (the default); ``concurrency`` instead wraps it in a
``@record_destination`` with that limit, which is how a pipeline sets one. The mock
holds each request for ``delay_ms`` and counts how many of a job's requests were open
at once (the job is tagged by its first record id), so a test can check the cap.
"""

import os

from typing_extensions import Annotated, NotRequired, Required

from typing import Any, Dict

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Record,
    Records,
    RecordDestination,
    RuntimeParams,
    Transformations,
    record_destination,
    transformation,
)
from reflowfy.destinations.api import api_destination
from tests.e2e.test_pipelines.sources import e2e_mock

MOCK_BASE_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook").removesuffix(
    "/webhook"
)


class ApiPerRecordParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EApiPerRecordPipeline`."""

    run: Annotated[Required[str], Param("Key the mock server files this run's requests under")]
    count: Annotated[NotRequired[int], Param("Records to generate", default=40)]
    batch_size: Annotated[NotRequired[int], Param("Records per job", default=20)]
    concurrency: Annotated[
        NotRequired[int],
        Param("Requests in flight per job via @record_destination (0: the default, 16)", default=0),
    ]
    delay_ms: Annotated[NotRequired[int], Param("Mock holds each request this long", default=0)]
    fail_id: Annotated[NotRequired[str], Param("Mock answers 503 for this id", default="")]
    use_body_field: Annotated[
        NotRequired[bool], Param("Send only record['payload'] (body_field)", default=False)
    ]


@transformation("per_record_shape")
def per_record_shape(records: Records, runtime_params: ApiPerRecordParams) -> Records:
    """Unwrap the mock source's ``{"data": item}`` to id/name; with use_body_field nest
    them under ``payload`` beside a field that must NOT be sent."""
    flat = [{"id": r["data"]["id"], "name": r["data"]["first_name"]} for r in records]
    if runtime_params.get("use_body_field"):
        return [{"id": r["id"], "payload": r, "internal": "x"} for r in flat]
    return flat


def _items_destination(params: ApiPerRecordParams, job: str) -> BaseDestination:
    return api_destination(
        url=f"{MOCK_BASE_URL}/items/{params['run']}/{{id}}",
        method="PUT",
        auth_type="bearer",
        auth_token="test-webhook-token",
        per_record=True,
        body_field="payload" if params.get("use_body_field") else None,
        params={
            "delay_ms": str(params.get("delay_ms", 0)),
            "fail_id": params.get("fail_id", ""),
            "job": job,
        },
    )


def _routed(record: Record, runtime_params: ApiPerRecordParams, job: str = "") -> BaseDestination:
    # The same destination for every record: one group, so the limit is the job's.
    return _items_destination(runtime_params, job)


# A @record_destination's limit is fixed where it is declared: one per value tested.
LIMITED: Dict[int, "type[RecordDestination]"] = {
    n: record_destination(f"e2e_api_per_record_c{n}", concurrency=n)(_routed)
    for n in (1, 4, 8, 10)
}


class E2EApiPerRecordPipeline(AbstractPipeline[ApiPerRecordParams]):
    """E2E pipeline for ``api_destination(per_record=True)``, alone or in a record destination."""

    name = "e2e_api_per_record_test"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params: ApiPerRecordParams):
        return e2e_mock(
            count=runtime_params.get("count", 40),
            batch_size=runtime_params.get("batch_size", 20),
        )

    def define_transformations(
        self, records: Records, runtime_params: ApiPerRecordParams
    ) -> Transformations:
        return [per_record_shape()]

    def define_destination(
        self, records: Records, runtime_params: ApiPerRecordParams
    ) -> BaseDestination:
        job = str(records[0]["id"]) if records else ""
        limit = runtime_params.get("concurrency", 0)
        if not limit:
            return _items_destination(runtime_params, job)
        limited: Any = LIMITED[limit](job=job)
        return limited
