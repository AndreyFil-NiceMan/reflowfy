"""E2E pipeline for per-record deduplication.

``define_jobs`` plans two jobs whose records overlap (ids 1-4 and 3-6), and a
transformation stamps ``sent_at`` so no two runs ever send byte-identical
records. ``dedup_key`` names what identifies a record instead: the run tag from
runtime_params plus the record id. So one run delivers ids 1-6 once each, and
repeating the run delivers nothing.

The api_destination body carries the records, so it declares ``dedup_identity``:
otherwise the body (and with it every record of the job) would be part of where
the job writes, and overlapping jobs would never match.
"""

import os
import time
from typing import Any, Iterable, List

from typing_extensions import Annotated, NotRequired

from reflowfy import (
    AbstractPipeline,
    BaseDestination,
    Param,
    Record,
    Records,
    RuntimeParams,
    Transformations,
    job,
    transformation,
)
from reflowfy.destinations.api import api_destination

MOCK_HTTP_URL = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook")


class RecordDedupParams(RuntimeParams, total=False):
    run: Annotated[NotRequired[str], Param("Run tag; a new tag is new work", default="r")]


def run_and_id(record: Record, runtime_params: RecordDedupParams) -> Any:
    return (runtime_params.get("run"), record["id"])


@transformation("e2e_stamp_sent_at")
def stamp_sent_at(records: Records, runtime_params: RecordDedupParams) -> Records:
    return [{**r, "sent_at": time.time()} for r in records]


class E2ERecordDedupPipeline(AbstractPipeline[RecordDedupParams]):
    """Overlapping jobs, each record sent once (enable_duplicate_jobs=False)."""

    name = "e2e_record_dedup"
    enable_duplicate_jobs = False
    dedup_key = staticmethod(run_and_id)

    def define_jobs(self, runtime_params: RecordDedupParams) -> Iterable[Any]:
        run = runtime_params.get("run", "r")
        # No per-job params: params given to job(...) are part of every record's hash,
        # so two jobs tagged differently would never dedup against each other.
        yield job([{"id": i, "run": run} for i in range(1, 5)])
        yield job([{"id": i, "run": run} for i in range(3, 7)])

    def define_transformations(
        self, records: List[Any], runtime_params: RecordDedupParams
    ) -> Transformations:
        return [stamp_sent_at()]

    def define_destination(
        self, records: List[Any], runtime_params: RecordDedupParams
    ) -> BaseDestination:
        return api_destination(
            url=MOCK_HTTP_URL,
            headers={"Content-Type": "application/json"},
            auth_type="bearer",
            auth_token="test-webhook-token",
            body={"records": records},
            dedup_identity={"url": MOCK_HTTP_URL},
        )
