"""E2E pipeline for the authoring API changed after 1.1.6 (#120, #121, #123).

Every step uses the new spelling, so a run proves it works end to end:

- ``@transformation`` used bare: the step is named after the function.
- ``@transformation(per_record=True)`` on a plain ``def``: ``None`` drops a record, a
  list replaces it, ``SkipRecord`` drops it with a reason; ``on_error="skip"`` drops
  a record whose call raised.
- Steps listed bare in ``define_transformations``; ``define_destination`` returning
  a bare ``@destination(per_record=True)`` class (default name).
- ``SkipRecord`` followed by ``SkipJob`` in the same job (#120).

Records are ``id = 1..20`` in jobs of 5; every delivery goes to the mock API's
``/gaps/{run}/{name}`` log (see ``framework_gaps_pipelines.gap_destination``).
"""

from typing import Any, Dict, List, Optional, Union

from typing_extensions import Annotated, NotRequired, Required

from reflowfy import (
    AbstractPipeline,
    Param,
    Record,
    Records,
    RuntimeParams,
    SkipJob,
    SkipRecord,
    destination,
    transformation,
)
from tests.e2e.test_pipelines.framework_gaps_pipelines import gap_destination
from tests.e2e.test_pipelines.sources import e2e_mock


class AuthoringParams(RuntimeParams, total=False):
    run: Required[str]
    scenario: Annotated[NotRequired[str], Param("steps | route | skip_then_drop", default="steps")]


@transformation
def e2e_auth_flatten(records: Records, runtime_params: AuthoringParams) -> Records:
    """Bare batch step: unwrap the mock source's ``{"data": ...}`` envelope."""
    return [r["data"] for r in records]


@transformation(per_record=True)
def e2e_auth_each(record: Record, runtime_params: AuthoringParams) -> Union[Record, List[Record], None]:
    """Plain per-record ``def``: every return the contract allows."""
    if record["id"] % 5 == 0:
        raise SkipRecord("multiple of five")
    if record["id"] % 3 == 0:
        return None  # dropped silently
    if record["id"] % 7 == 0:
        return [record, {"id": record["id"] + 1000}]  # one record in, two out
    return record


@transformation(name="e2e_auth_fragile_v2", per_record=True, on_error="skip")
def e2e_auth_fragile(record: Record, runtime_params: AuthoringParams) -> Record:
    """``on_error="skip"``: the record whose call raises is dropped, its type the reason."""
    if record["id"] == 1:
        raise KeyError("missing")
    return record


@transformation(per_record=True)
def e2e_auth_skip_odd(record: Record, runtime_params: AuthoringParams) -> Optional[Record]:
    if record["id"] % 2:
        raise SkipRecord("odd")
    return record


@transformation
def e2e_auth_drop_first_job(records: Records, runtime_params: AuthoringParams) -> Records:
    if any(r["id"] <= 5 for r in records):
        raise SkipJob("first job")
    return records


@destination(per_record=True)
def e2e_auth_by_parity(record: Record, runtime_params: AuthoringParams) -> Any:
    """Routes each record by parity: two destinations per job."""
    return gap_destination(runtime_params["run"], "even" if record["id"] % 2 == 0 else "odd")


class E2EAuthoringApiPipeline(AbstractPipeline[AuthoringParams]):
    name = "e2e_authoring_api"

    def define_source(self, runtime_params: AuthoringParams) -> Any:
        return e2e_mock(data=[{"id": i} for i in range(1, 21)], batch_size=5)

    def define_transformations(self, records: Records, runtime_params: AuthoringParams) -> Any:
        scenario = runtime_params.get("scenario", "steps")
        if scenario == "route":
            return [e2e_auth_flatten]
        if scenario == "skip_then_drop":
            return [e2e_auth_flatten, e2e_auth_skip_odd, e2e_auth_drop_first_job]
        return [e2e_auth_flatten, e2e_auth_each, e2e_auth_fragile]

    def define_destination(self, records: Records, runtime_params: AuthoringParams) -> Any:
        if runtime_params.get("scenario") == "route":
            return e2e_auth_by_parity
        return gap_destination(runtime_params["run"], "out")


def expected_steps_ids() -> List[int]:
    """What the ``steps`` scenario delivers (mirrors ``e2e_auth_each``/``_fragile``)."""
    out: List[int] = []
    for i in range(1, 21):
        if i % 5 == 0 or i % 3 == 0 or i == 1:
            continue
        out.append(i)
        if i % 7 == 0:
            out.append(i + 1000)
    return sorted(out)


EXPECTED_STEPS_SKIPS: Dict[str, int] = {"multiple of five": 4, "KeyError": 1}
