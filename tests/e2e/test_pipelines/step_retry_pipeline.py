"""E2E pipeline for step retries (``RetryConfig``): a source, a per-record and a batch
transformation, and a destination that each call the mock API.

Every call goes to ``/gaps/{run}/{step}`` (``tests/e2e/destinations/mock_api_server.py``),
which answers 503 to the first ``fail_first`` requests of that key and logs each one, so
a test sees how often every step was called. ``*_fail`` params set ``fail_first`` per
step; 0 (the default) never fails.
"""

import os
from typing import Any, Dict, List, Optional

import httpx
from typing_extensions import Annotated, NotRequired

from reflowfy import (
    AbstractPipeline,
    BaseSource,
    Param,
    RetryConfig,
    RuntimeParams,
    api_destination,
    transformation,
)
from reflowfy.core.exceptions import SourceError

MOCK_BASE = os.getenv("MOCK_HTTP_URL", "http://reflofy-e2e-mock-http:8091/webhook").rsplit(
    "/webhook", 1
)[0]

# Short waits so a test is quick; three attempts per step.
STEP_RETRY = RetryConfig(max_attempts=3, min_wait_seconds=0.2, max_wait_seconds=1, multiplier=0.1)


def step_url(run: str, step: str, fail_first: int) -> str:
    return f"{MOCK_BASE}/gaps/{run}/{step}?fail_first={fail_first}"


class E2EFlakyHttpSource(BaseSource):
    """``count`` records, after a POST to the mock that may answer 503 (a ``SourceError``).

    One job: ``split`` is the default. Its retry is set with ``with_retry`` at planning,
    so it reaches the job only through the job message.
    """

    def __init__(self, url: str, count: int = 3):
        super().__init__({"url": url, "count": count})

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        response = httpx.post(self.config["url"], json={"step": "source"}, timeout=10)
        if response.status_code >= 300:
            raise SourceError("e2e_flaky_http", f"HTTP {response.status_code}", None)
        return [{"id": n} for n in range(self.config["count"])]

    def health_check(self) -> bool:
        return True


class StepRetryParams(RuntimeParams, total=False):
    run: str
    count: Annotated[NotRequired[int], Param(default=3)]
    source_fail: Annotated[NotRequired[int], Param(default=0)]
    record_fail: Annotated[NotRequired[int], Param(default=0)]
    batch_fail: Annotated[NotRequired[int], Param(default=0)]
    dest_fail: Annotated[NotRequired[int], Param(default=0)]


def _post(url: str, body: Any) -> None:
    """POST and raise ``httpx.HTTPStatusError`` on a 503 (retried: see ``retry_on``)."""
    httpx.post(url, json=body, timeout=10).raise_for_status()


# raise_for_status() raises HTTPStatusError, which the step defaults do not retry (a 4xx
# is often permanent); these steps opt in for it.
@transformation(
    "e2e_step_retry_record",
    per_record=True,
    concurrency=1,  # one record at a time: which requests fail is deterministic
    retry=RetryConfig(
        max_attempts=3, min_wait_seconds=0.2, multiplier=0.1, retry_on=(httpx.HTTPStatusError,)
    ),
)
async def lookup_record(record: Dict[str, Any], runtime_params: StepRetryParams) -> Dict[str, Any]:
    url = step_url(runtime_params.get("run", "x"), "record", runtime_params.get("record_fail", 0))
    async with httpx.AsyncClient(timeout=10) as client:
        (await client.post(url, json=record)).raise_for_status()
    return {**record, "looked_up": True}


@transformation(
    "e2e_step_retry_batch",
    retry=RetryConfig(
        max_attempts=3, min_wait_seconds=0.2, multiplier=0.1, retry_on=(httpx.HTTPStatusError,)
    ),
)
def mark_batch(records: List[Dict[str, Any]], runtime_params: StepRetryParams) -> List[Any]:
    url = step_url(runtime_params.get("run", "x"), "batch", runtime_params.get("batch_fail", 0))
    _post(url, {"records": len(records)})
    return [{**record, "batched": True} for record in records]


class StepRetryPipeline(AbstractPipeline[StepRetryParams]):
    name = "e2e_step_retry"

    def define_source(self, runtime_params: StepRetryParams) -> BaseSource:
        run = runtime_params.get("run", "x")
        url = step_url(run, "source", runtime_params.get("source_fail", 0))
        return E2EFlakyHttpSource(url, runtime_params.get("count", 3)).with_retry(STEP_RETRY)

    def define_transformations(self, records: List[Any], runtime_params: StepRetryParams) -> Any:
        return [lookup_record, mark_batch]

    def define_destination(self, records: List[Any], runtime_params: StepRetryParams) -> Any:
        return api_destination(
            url=f"{MOCK_BASE}/gaps/{runtime_params.get('run', 'x')}/dest",
            params={"fail_first": str(runtime_params.get("dest_fail", 0))},
            per_record=True,
            retry_config=STEP_RETRY,  # a 503 is a DestinationError: retried by default
        )
