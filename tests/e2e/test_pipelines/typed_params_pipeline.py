"""
Typed runtime_params E2E test pipeline.

Verifies the typed-params DX end to end, through the real dispatch path:

  1. `define_parameters()` is derived from the params TypedDict, so validation
     (required, choices) and defaults work without a hand-written declaration.
  2. A declared default lands in runtime_params when the caller omits it.
  3. `Literal` choices are rejected by the API when violated.
  4. Enrichment still works when the written keys are declared: define_source
     writes one, a transformation writes another, and both reach the destination.

Everything the run observed is echoed to the mock HTTP server in the body, so
the test can assert on it.
"""

from typing import Any, Dict, List, Literal, Sequence

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import AbstractPipeline, Param, RuntimeParams, transformation
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
from tests.e2e.test_pipelines.destinations import e2e_http_runtime_params
from tests.e2e.test_pipelines.sources import e2e_mock


class TypedParams(RuntimeParams, total=False):
    """Declared once: this types the hooks AND becomes define_parameters().

    - ``env`` is required and constrained, so the API rejects a bad value.
    - ``multiplier`` carries a default, applied when the caller omits it.
    - ``label`` is a plain optional string.
    - the last two are enrichment keys this pipeline's own code writes.
    """

    env: Required[Literal["dev", "prod"]]
    multiplier: Annotated[NotRequired[int], Param("Value multiplier", default=3)]
    label: NotRequired[str]

    # Written at runtime, not supplied by the caller — declared so that the code
    # reading them downstream sees their types.
    derived_label: Annotated[str, Param(internal=True)]
    multiplied_count: Annotated[int, Param(internal=True)]


@transformation("typed_params_multiply")
def typed_params_multiply(records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
    """Multiply each record's value by the (possibly defaulted) multiplier."""
    multiplier = runtime_params.get("multiplier", 1)
    for record in records:
        record["value"] = record.get("value", 1) * multiplier
        record["_env"] = runtime_params.get("env")
        record["_execution_id"] = runtime_params.get("execution_id")
    runtime_params["multiplied_count"] = len(records)
    return records


class E2ETypedParamsPipeline(AbstractPipeline[TypedParams]):
    """Pipeline whose parameters are declared as types rather than by hand."""

    name = "e2e_typed_params"
    rate_limit = 3000  # jobs per minute

    # No define_parameters(): it is derived from TypedParams.

    def define_source(self, runtime_params: TypedParams) -> BaseSource:
        # Reading a declared key: the IDE knows env is "dev" | "prod".
        runtime_params["derived_label"] = f"{runtime_params.get('label', 'none')}"
        return e2e_mock(count=10, batch_size=10)

    def define_destination(
        self, records: List[Any], runtime_params: TypedParams
    ) -> BaseDestination:
        return e2e_http_runtime_params(body={"records": records, "runtime_params": runtime_params})

    def define_transformations(
        self, records: List[Any], runtime_params: TypedParams
    ) -> Sequence[BaseTransformation]:
        return [typed_params_multiply()]
