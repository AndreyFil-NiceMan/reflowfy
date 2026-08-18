"""
Custom Job Splitting Test Pipeline.

Pipeline that calls an API from ``define_jobs`` and splits the response into
jobs with its own logic. Used for E2E testing of the ``define_jobs`` hook and
``chunk(records, size=N)``.
"""

import os
from typing import Any, Dict, Iterable, List, Sequence

import httpx
from typing_extensions import Annotated, NotRequired

from reflowfy import AbstractPipeline, Param, RuntimeParams, chunk
from reflowfy.destinations.base import BaseDestination
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_console

# Inside Docker: http://e2e-mock-api:8092 — outside Docker defaults to localhost:8092
MOCK_API_URL = os.getenv("MOCK_API_URL", "http://localhost:8092")


class DefineJobsParams(RuntimeParams, total=False):
    """Params for :class:`E2EDefineJobsPipeline`."""

    base_url: Annotated[NotRequired[str], Param("Base API URL", default=MOCK_API_URL)]
    job_size: Annotated[NotRequired[int], Param("Records per job", default=1)]


class E2EDefineJobsPipeline(AbstractPipeline[DefineJobsParams]):
    """E2E test pipeline that owns its job splitting.

    ``GET /objects`` returns ``{"obj1": 1, "obj2": 2, "obj3": 3}`` — 3 records,
    so ``job_size=1`` dispatches 3 jobs and ``job_size=2`` dispatches 2.
    """

    name = "e2e_define_jobs"
    rate_limit = 600  # jobs per minute

    # define_parameters() is derived from DefineJobsParams.

    def define_jobs(self, runtime_params: DefineJobsParams) -> Iterable[Any]:
        base_url = runtime_params.get("base_url", MOCK_API_URL)
        data: Dict[str, Any] = httpx.get(f"{base_url}/objects", timeout=10.0).json()
        records = [{"name": key, "value": value} for key, value in data.items()]
        return chunk(records, size=runtime_params.get("job_size", 1))

    def define_destination(
        self, records: List[Any], runtime_params: DefineJobsParams
    ) -> BaseDestination:
        return e2e_console()

    def define_transformations(
        self, records: List[Any], runtime_params: DefineJobsParams
    ) -> Sequence[BaseTransformation]:
        return []
