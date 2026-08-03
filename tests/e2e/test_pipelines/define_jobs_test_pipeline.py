"""
Custom Job Splitting Test Pipeline.

Pipeline that calls an API from ``define_jobs`` and splits the response into
jobs with its own logic. Used for E2E testing of the ``define_jobs`` hook and
``chunk(records, size=N)``.
"""

import os

import httpx

from reflowfy import AbstractPipeline, PipelineParameter, chunk
from tests.e2e.test_pipelines.destinations import e2e_console

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class E2EDefineJobsPipeline(AbstractPipeline):
    """E2E test pipeline that owns its job splitting.

    ``GET /objects`` returns ``{"obj1": 1, "obj2": 2, "obj3": 3}`` — 3 records,
    so ``job_size=1`` dispatches 3 jobs and ``job_size=2`` dispatches 2.
    """

    name = "e2e_define_jobs"
    rate_limit = 600  # jobs per minute

    def define_parameters(self):
        return [
            PipelineParameter(
                name="base_url",
                description="Base API URL",
                param_type=str,
                required=False,
                default=MOCK_API_URL,
            ),
            PipelineParameter(
                name="job_size",
                description="Records per job",
                param_type=int,
                required=False,
                default=1,
            ),
        ]

    def define_jobs(self, runtime_params):
        base_url = runtime_params.get("base_url", MOCK_API_URL)
        data = httpx.get(f"{base_url}/objects", timeout=10.0).json()
        records = [{"name": key, "value": value} for key, value in data.items()]
        return chunk(records, size=runtime_params.get("job_size", 1))

    def define_destination(self, records, runtime_params):
        return e2e_console()

    def define_transformations(self, records, runtime_params):
        return []
