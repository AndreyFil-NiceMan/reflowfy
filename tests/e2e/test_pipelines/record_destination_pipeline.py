"""
Kafka destination chosen per record, with ``@destination(per_record=True)``.

Generated records, each stamped with the test's ``run`` key and a ``region``
(``eu`` / ``us`` / ``apac`` by id), go to one topic per region: one job
writes to three topics. ``scenario`` picks the destination function:

- ``route``: every record has a known region;
- ``skip``: every 5th record's region is unknown, and ``on_error="skip"`` drops it
  (reason ``KeyError``) while the rest of the job is sent;
- ``fail``: the same unknown regions with the default ``on_error="fail"``, so every
  job containing one fails before it writes anything.

The test consumes the three topics and checks where each record landed.
"""

from reflowfy import destination
import os
from typing import Literal

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
from reflowfy.destinations.kafka import kafka_destination
from tests.e2e.test_pipelines.sources import e2e_mock

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",  # Docker-internal PLAINTEXT (set by run script)
    os.getenv("E2E_KAFKA_SERVERS", "127.0.0.1:9095"),
)
REGIONS = ("eu", "us", "apac")
REGION_TOPICS = {region: f"e2e-rd-{region}" for region in REGIONS}


class RecordDestinationParams(RuntimeParams, total=False):
    """Parameters for :class:`E2ERecordDestinationPipeline`."""

    run: Annotated[Required[str], Param("Stamped on every record, so a test finds its own")]
    scenario: Annotated[
        NotRequired[Literal["route", "skip", "fail"]],
        Param("Which destination function routes the records", default="route"),
    ]
    count: Annotated[NotRequired[int], Param("Records to generate", default=30)]
    batch_size: Annotated[NotRequired[int], Param("Records per job", default=15)]


@transformation("stamp_region")
def stamp_region(records: Records, runtime_params: RecordDestinationParams) -> Records:
    unknown = runtime_params.get("scenario", "route") != "route"
    stamped: Records = []
    for r in records:
        record_id: int = r["data"]["id"]
        region: str = "unknown" if unknown and record_id % 5 == 0 else REGIONS[record_id % 3]
        stamped.append({"id": record_id, "run": runtime_params["run"], "region": region})
    return stamped


def _region_destination(record: Record) -> BaseDestination:
    # KeyError for an region with no topic: the per-record failure under test.
    return kafka_destination(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=REGION_TOPICS[record["region"]]
    )


@destination("e2e_region_topics", per_record=True)
def region_topics(record: Record, runtime_params: RecordDestinationParams) -> BaseDestination:
    return _region_destination(record)


@destination("e2e_region_topics_skip", per_record=True, on_error="skip")
def region_topics_skip(
    record: Record, runtime_params: RecordDestinationParams
) -> BaseDestination:
    return _region_destination(record)


class E2ERecordDestinationPipeline(AbstractPipeline[RecordDestinationParams]):
    """E2E pipeline for ``@destination(per_record=True)``: one job, many topics."""

    name = "e2e_record_destination_test"
    rate_limit = 6000  # jobs per minute

    def define_source(self, runtime_params: RecordDestinationParams):
        return e2e_mock(
            count=runtime_params.get("count", 30),
            batch_size=runtime_params.get("batch_size", 15),
        )

    def define_transformations(
        self, records: Records, runtime_params: RecordDestinationParams
    ) -> Transformations:
        return [stamp_region()]

    def define_destination(
        self, records: Records, runtime_params: RecordDestinationParams
    ) -> BaseDestination:
        if runtime_params.get("scenario", "route") == "skip":
            return region_topics_skip()
        return region_topics()
