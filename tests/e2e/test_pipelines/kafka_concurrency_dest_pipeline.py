"""
Kafka destination with ``concurrency``.

Generated records, each stamped with the test's ``run`` key, go to one topic with
``concurrency`` messages in flight per job. The test consumes the topic and checks that
every record of its run arrived exactly once (and, at concurrency 1, in order).
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
from reflowfy.destinations.kafka import kafka_destination
from tests.e2e.test_pipelines.sources import e2e_mock

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",  # Docker-internal PLAINTEXT (set by run script)
    os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095"),
)
CONCURRENCY_TOPIC = os.getenv("E2E_KAFKA_CONCURRENCY_TOPIC", "e2e-kafka-concurrency")


class KafkaConcurrencyParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EKafkaConcurrencyPipeline`."""

    run: Annotated[Required[str], Param("Stamped on every record, so a test finds its own")]
    count: Annotated[NotRequired[int], Param("Records to generate", default=200)]
    batch_size: Annotated[NotRequired[int], Param("Records per job", default=50)]
    concurrency: Annotated[NotRequired[int], Param("Messages in flight per job", default=1)]


@transformation("stamp_run")
def stamp_run(records: Records, runtime_params: KafkaConcurrencyParams) -> Records:
    # The mock source wraps every item as {"data": item}.
    return [{"id": r["data"]["id"], "run": runtime_params["run"]} for r in records]


class E2EKafkaConcurrencyPipeline(AbstractPipeline[KafkaConcurrencyParams]):
    """E2E pipeline for ``kafka_destination(concurrency=...)``."""

    name = "e2e_kafka_concurrency_test"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params: KafkaConcurrencyParams):
        return e2e_mock(
            count=runtime_params.get("count", 200),
            batch_size=runtime_params.get("batch_size", 50),
        )

    def define_transformations(
        self, records: Records, runtime_params: KafkaConcurrencyParams
    ) -> Transformations:
        return [stamp_run()]

    def define_destination(
        self, records: Records, runtime_params: KafkaConcurrencyParams
    ) -> BaseDestination:
        return kafka_destination(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=CONCURRENCY_TOPIC,
            concurrency=runtime_params.get("concurrency", 1),
        )
