"""
API destination in per-record mode.

Every record goes in its own ``PUT /items/<run>/<id>`` to the mock webhook server:
the record's ``id`` fills the URL placeholder, and the record (or only its ``payload``
with ``use_key_field``) is the body. ``concurrency`` requests are in flight per job;
the mock holds each one for ``delay_ms`` and counts how many of a job's requests were
open at once (the job is tagged by its first record id), so a test can check the cap.
"""

import os

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Records,
    RuntimeParams,
    Transformations,
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
    concurrency: Annotated[NotRequired[int], Param("Requests in flight per job", default=1)]
    delay_ms: Annotated[NotRequired[int], Param("Mock holds each request this long", default=0)]
    fail_id: Annotated[NotRequired[str], Param("Mock answers 503 for this id", default="")]
    use_key_field: Annotated[
        NotRequired[bool], Param("Send only record['payload'] (key_field)", default=False)
    ]


@transformation("per_record_shape")
def per_record_shape(records: Records, runtime_params: ApiPerRecordParams) -> Records:
    """Unwrap the mock source's ``{"data": item}`` to id/name; with use_key_field nest
    them under ``payload`` beside a field that must NOT be sent."""
    flat = [{"id": r["data"]["id"], "name": r["data"]["first_name"]} for r in records]
    if runtime_params.get("use_key_field"):
        return [{"id": r["id"], "payload": r, "internal": "x"} for r in flat]
    return flat


class E2EApiPerRecordPipeline(AbstractPipeline[ApiPerRecordParams]):
    """E2E pipeline for ``api_destination(per_record=True, concurrency=...)``."""

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
        return api_destination(
            url=f"{MOCK_BASE_URL}/items/{runtime_params['run']}/{{id}}",
            method="PUT",
            auth_type="bearer",
            auth_token="test-webhook-token",
            per_record=True,
            key_field="payload" if runtime_params.get("use_key_field") else None,
            concurrency=runtime_params.get("concurrency", 1),
            params={
                "delay_ms": str(runtime_params.get("delay_ms", 0)),
                "fail_id": runtime_params.get("fail_id", ""),
                "job": str(records[0]["id"]) if records else "",
            },
        )
