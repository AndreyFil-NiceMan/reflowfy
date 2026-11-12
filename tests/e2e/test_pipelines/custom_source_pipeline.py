"""
Custom Source Test Pipeline.

The source is defined here, in the user's code, not in reflowfy: it registers itself
with SourceFactory when the class is defined, so the worker (or the in-process
LocalDispatcher) rebuilds each planned slice from the job message. Its ``api_key``
is a literal credential, so the job rows returned by the API must show it redacted.
"""

import math
from typing import Any, Dict, Iterator, List, Optional, Sequence

from typing_extensions import Annotated, NotRequired

from reflowfy import AbstractPipeline, Param, RuntimeParams
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_console


class E2ERangeSource(BaseSource):
    """Numbers ``start <= n < stop``; ``split`` plans ``per_job`` numbers per job."""

    secret_fields = ("api_key",)

    def __init__(self, start: int, stop: int, per_job: int = 10, api_key: str = ""):
        super().__init__({"start": start, "stop": stop, "per_job": per_job, "api_key": api_key})

    def split(self, runtime_params: Dict[str, Any]) -> Iterator["E2ERangeSource"]:
        c = self.config
        for i in range(math.ceil((c["stop"] - c["start"]) / c["per_job"])):
            lo = c["start"] + i * c["per_job"]
            yield E2ERangeSource(lo, min(lo + c["per_job"], c["stop"]), c["per_job"], c["api_key"])

    def fetch(self, runtime_params: Dict[str, Any], limit: Optional[int] = None) -> List[Any]:
        numbers = range(self.config["start"], self.config["stop"])
        return [{"n": n} for n in numbers][:limit]

    def health_check(self) -> bool:
        return True


class CustomSourceParams(RuntimeParams, total=False):
    """Params for :class:`E2ECustomSourcePipeline`."""

    count: Annotated[NotRequired[int], Param("Numbers to emit", default=25)]
    per_job: Annotated[NotRequired[int], Param("Numbers per job", default=10)]


class E2ECustomSourcePipeline(AbstractPipeline[CustomSourceParams]):
    """25 numbers in jobs of 10 by default: 3 jobs."""

    name = "e2e_custom_source"
    rate_limit = 600

    def define_source(self, runtime_params: CustomSourceParams) -> BaseSource:
        return E2ERangeSource(
            0,
            runtime_params.get("count", 25),
            runtime_params.get("per_job", 10),
            api_key="e2e-literal-key",
        )

    def define_transformations(
        self, records: List[Any], runtime_params: CustomSourceParams
    ) -> Sequence[BaseTransformation]:
        return []

    def define_destination(
        self, records: List[Any], runtime_params: CustomSourceParams
    ) -> BaseDestination:
        return e2e_console()
