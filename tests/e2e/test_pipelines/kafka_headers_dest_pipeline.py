"""
Kafka destination with ``headers``.

Generated records, each stamped with the test's ``run`` key, go to one topic with
message headers set in one of three ways (``mode``):

- ``static``: a mapping with ``{field}`` placeholders filled from each record;
- ``function``: a function of the record returning str / bytes / None values;
- ``missing``: a placeholder naming a field no record has, so every job fails
  before it writes anything.

The test consumes the topic and checks each message's headers.
"""

import os
from typing import Dict, Literal, Optional

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Record,
    Records,
    RuntimeParams,
    Transformations,
    transformation,
)
from reflowfy.destinations.kafka import HeaderValue, KafkaHeaders, kafka_destination
from tests.e2e.test_pipelines.sources import e2e_mock

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",  # Docker-internal PLAINTEXT (set by run script)
    os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095"),
)
HEADERS_TOPIC = os.getenv("E2E_KAFKA_HEADERS_TOPIC", "e2e-kafka-headers")


class KafkaHeadersParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EKafkaHeadersPipeline`."""

    run: Annotated[Required[str], Param("Stamped on every record, so a test finds its own")]
    mode: Annotated[
        NotRequired[Literal["static", "function", "missing"]],
        Param("How the headers are given", default="static"),
    ]
    count: Annotated[NotRequired[int], Param("Records to generate", default=20)]
    batch_size: Annotated[NotRequired[int], Param("Records per job", default=10)]
    concurrency: Annotated[NotRequired[int], Param("Messages in flight per job", default=1)]


@transformation("stamp_headers_run")
def stamp_headers_run(records: Records, runtime_params: KafkaHeadersParams) -> Records:
    # The mock source wraps every item as {"data": item}.
    return [{"id": r["data"]["id"], "run": runtime_params["run"]} for r in records]


def _function_headers(record: Record) -> Optional[Dict[str, HeaderValue]]:
    return {
        "record-id": str(record["id"]),
        "parity": "even" if record["id"] % 2 == 0 else "odd",
        "raw": b"\x00\xff",
        "nullable": None,
    }


class E2EKafkaHeadersPipeline(AbstractPipeline[KafkaHeadersParams]):
    """E2E pipeline for ``kafka_destination(headers=...)``."""

    name = "e2e_kafka_headers_test"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params: KafkaHeadersParams):
        return e2e_mock(
            count=runtime_params.get("count", 20),
            batch_size=runtime_params.get("batch_size", 10),
        )

    def define_transformations(
        self, records: Records, runtime_params: KafkaHeadersParams
    ) -> Transformations:
        return [stamp_headers_run()]

    def define_destination(
        self, records: Records, runtime_params: KafkaHeadersParams
    ) -> BaseDestination:
        mode = runtime_params.get("mode", "static")
        headers: KafkaHeaders
        if mode == "function":
            headers = _function_headers
        elif mode == "missing":
            headers = {"tenant": "{tenant_id}"}
        else:
            headers = {"source": "reflowfy-e2e", "run": "{run}", "record-id": "{id}"}
        return kafka_destination(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=HEADERS_TOPIC,
            concurrency=runtime_params.get("concurrency", 1),
            headers=headers,
        )
