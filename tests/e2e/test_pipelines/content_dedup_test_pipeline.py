"""E2E pipeline for worker-side content deduplication tests.

Records are derived from runtime_params['payload'] and embedded in the
e2e_mock source descriptor, so two runs with the same payload fetch
identical records (worker dedups the second), and a changed payload
produces different records (worker re-processes).
"""

from reflowfy import AbstractPipeline
from tests.e2e.test_pipelines.sources import e2e_mock
from tests.e2e.test_pipelines.destinations import e2e_http


class E2EContentDedupPipeline(AbstractPipeline):
    """Worker-side content dedup pipeline (enable_duplicate_jobs=False)."""

    name = "e2e_content_dedup"
    enable_duplicate_jobs = False

    def define_source(self, runtime_params):
        payload = runtime_params.get("payload", "A")
        data = [{"id": 1, "payload": payload}]
        return e2e_mock(data=data, batch_size=1)

    def define_destination(self, records, runtime_params):
        return e2e_http(body={"records": records})

    def define_transformations(self, records, runtime_params):
        return []
