"""A pipeline with no params of its own migrates with one token."""

from typing import Any, List

from reflowfy import AbstractPipeline, BaseDestination, BaseSource, RuntimeParams, Transformations
from reflowfy.destinations.console import ConsoleDestination
from reflowfy.sources.static import StaticSource


class PlainPipeline(AbstractPipeline[RuntimeParams]):
    name = "typecheck_pass_plain"

    def define_source(self, runtime_params: RuntimeParams) -> BaseSource:
        execution_id = runtime_params.get("execution_id")  # reserved keys via .get()
        return StaticSource([{"id": 1, "execution_id": execution_id}])

    def define_transformations(
        self, records: List[Any], runtime_params: RuntimeParams
    ) -> Transformations:
        return []

    def define_destination(
        self, records: List[Any], runtime_params: RuntimeParams
    ) -> BaseDestination:
        return ConsoleDestination()
