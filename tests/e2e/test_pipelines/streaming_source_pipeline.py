"""
Streaming sources and async pipeline hooks.

``kind="generator"``: the source's ``fetch`` ``yield``-s its records — it used to be
``str()``-ed and sent one character per record. ``kind="async"``: an ``AsyncSource``
whose ``fetch_async`` the worker awaits. Every job-side hook is ``async def``;
``should_apply_transformation`` decides whether ``keep_even`` runs, so a job's
``processed_records`` shows the decision was honoured (an async hook returning False
used to be ignored).
"""

import asyncio
import math
from typing import Any, Dict, Iterator, List, Optional, Sequence

from typing_extensions import Annotated, Literal, NotRequired

from reflowfy import AbstractPipeline, AsyncSource, Param, RuntimeParams, transformation
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_console


def _windows(count: int, per_job: int) -> Iterator[Dict[str, int]]:
    for i in range(math.ceil(count / per_job)):
        yield {"start": i * per_job, "stop": min((i + 1) * per_job, count)}


class E2EYieldingSource(BaseSource):
    """Numbers ``start <= n < stop``, yielded one at a time."""

    def __init__(self, start: int, stop: int, per_job: int = 10):
        super().__init__({"start": start, "stop": stop, "per_job": per_job})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["E2EYieldingSource"]:
        per_job = self.config["per_job"]
        for w in _windows(self.config["stop"], per_job):
            yield E2EYieldingSource(w["start"], w["stop"], per_job)

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> Iterator[Any]:
        for n in range(self.config["start"], self.config["stop"]):
            yield {"n": n}

    def health_check(self) -> bool:
        return True


class E2EAsyncRangeSource(AsyncSource):
    """The same numbers from an ``async def fetch_async``."""

    def __init__(self, start: int, stop: int, per_job: int = 10):
        super().__init__({"start": start, "stop": stop, "per_job": per_job})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["E2EAsyncRangeSource"]:
        per_job = self.config["per_job"]
        for w in _windows(self.config["stop"], per_job):
            yield E2EAsyncRangeSource(w["start"], w["stop"], per_job)

    async def fetch_async(
        self, runtime_params: Dict[str, Any], limit: Optional[int] = None
    ) -> List[Any]:
        await asyncio.sleep(0.01)  # stands in for an awaited HTTP call
        return [{"n": n} for n in range(self.config["start"], self.config["stop"])]

    def health_check(self) -> bool:
        return True


@transformation("e2e_keep_even")
def keep_even(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    return [r for r in records if r["n"] % 2 == 0]


class StreamingParams(RuntimeParams, total=False):
    """Params for :class:`E2EStreamingSourcePipeline`."""

    kind: Annotated[NotRequired[Literal["generator", "async"]], Param("Source kind", default="generator")]
    count: Annotated[NotRequired[int], Param("Numbers to emit", default=25)]
    per_job: Annotated[NotRequired[int], Param("Numbers per job", default=10)]
    even_only: Annotated[NotRequired[bool], Param("Apply keep_even", default=False)]


class E2EStreamingSourcePipeline(AbstractPipeline[StreamingParams]):
    """25 numbers in jobs of 10 by default: 3 jobs."""

    name = "e2e_streaming_source"
    rate_limit = 600

    def define_source(self, runtime_params: StreamingParams) -> BaseSource:
        count, per_job = runtime_params.get("count", 25), runtime_params.get("per_job", 10)
        if runtime_params.get("kind", "generator") == "async":
            return E2EAsyncRangeSource(0, count, per_job)
        return E2EYieldingSource(0, count, per_job)

    async def define_transformations(
        self, records: List[Any], runtime_params: StreamingParams
    ) -> Sequence[BaseTransformation]:
        await asyncio.sleep(0)
        return [keep_even()]

    async def should_apply_transformation(
        self, transformation: BaseTransformation, runtime_params: StreamingParams, records: List[Any]
    ) -> bool:
        await asyncio.sleep(0)
        return bool(runtime_params.get("even_only", False))

    async def define_destination(
        self, records: List[Any], runtime_params: StreamingParams
    ) -> BaseDestination:
        await asyncio.sleep(0)
        return e2e_console()
