"""A typed hook that returns the wrong kind of object is caught."""

from typing import Any, List

from reflowfy import AbstractPipeline, BaseDestination, BaseSource, RuntimeParams
from reflowfy.sources.static import StaticSource


class BadReturnPipeline(AbstractPipeline[RuntimeParams]):
    name = "typecheck_fail_return"

    def define_source(self, runtime_params: RuntimeParams) -> BaseSource:
        return StaticSource([])

    def define_destination(
        self, records: List[Any], runtime_params: RuntimeParams
    ) -> BaseDestination:
        return StaticSource([])  # expect: reportReturnType
