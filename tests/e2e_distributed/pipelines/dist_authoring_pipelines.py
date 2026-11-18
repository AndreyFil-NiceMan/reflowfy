"""Distributed E2E pipeline for the authoring API changed after 1.1.6 (#120, #121, #123).

Same conventions as ``dist_pipelines.py``: ``run`` is the sink channel, ``sink_url`` the
sink as the workers see it, records are generated (``id = 1..total``) and planned with
``define_jobs``, so every step below runs on a real worker after a trip over Kafka.

- ``@transformation`` used bare (step named after the function), listed bare.
- ``@transformation(per_record=True)`` on a plain ``def``: ``None`` drops, a list
  replaces, ``SkipRecord`` drops with a reason; ``on_error="skip"`` with ``name=``.
- ``define_destination`` returning a bare ``@destination(per_record=True)`` class.
- ``SkipRecord`` then ``SkipJob`` in one job (#120).

Copied into a project with ``sync_pipelines.sh``.
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
    api_destination,
    chunk,
    destination,
    transformation,
)

SINK = "http://e2e-sink:8080"


class AuthoringParams(RuntimeParams, total=False):
    run: Required[str]
    sink_url: Annotated[NotRequired[str], Param("Sink base URL as the workers see it", default=SINK)]
    total: Annotated[NotRequired[int], Param("Records", default=20)]
    per_job: Annotated[NotRequired[int], Param("Records per job", default=5)]
    scenario: Annotated[NotRequired[str], Param("steps | route | skip_then_drop", default="steps")]


def _sink(params: Any) -> str:
    return str(params.get("sink_url", SINK))


@transformation
def dist_auth_tag(records: Records, runtime_params: AuthoringParams) -> Records:
    """Bare batch step: tags each record with the worker-side step that saw it."""
    return [{**r, "tag": "batch"} for r in records]


@transformation(per_record=True)
def dist_auth_each(record: Record, runtime_params: AuthoringParams) -> Union[Record, List[Record], None]:
    if record["id"] % 5 == 0:
        raise SkipRecord("multiple of five")
    if record["id"] % 3 == 0:
        return None
    if record["id"] % 7 == 0:
        return [record, {"id": record["id"] + 1000, "tag": record["tag"]}]
    return record


@transformation(name="dist_auth_fragile_v2", per_record=True, on_error="skip")
def dist_auth_fragile(record: Record, runtime_params: AuthoringParams) -> Record:
    if record["id"] == 1:
        raise KeyError("missing")
    return record


@transformation(per_record=True)
def dist_auth_skip_odd(record: Record, runtime_params: AuthoringParams) -> Optional[Record]:
    if record["id"] % 2:
        raise SkipRecord("odd")
    return record


@transformation
def dist_auth_drop_first_job(records: Records, runtime_params: AuthoringParams) -> Records:
    if any(r["id"] <= int(runtime_params.get("per_job", 5)) for r in records):
        raise SkipJob("first job")
    return records


@destination(per_record=True)
def dist_auth_by_parity(record: Record, runtime_params: AuthoringParams) -> Any:
    parity = "even" if record["id"] % 2 == 0 else "odd"
    return api_destination(
        url=f"{_sink(runtime_params)}/record/{runtime_params['run']}-{parity}/{{id}}",
        method="PUT",
        per_record=True,
    )


class DistAuthoring(AbstractPipeline[AuthoringParams]):
    name = "dist_authoring"

    def define_jobs(self, runtime_params: AuthoringParams) -> Any:
        records = [{"id": i} for i in range(1, int(runtime_params.get("total", 20)) + 1)]
        return chunk(records, size=int(runtime_params.get("per_job", 5)))

    def define_transformations(self, records: Records, runtime_params: AuthoringParams) -> Any:
        scenario = runtime_params.get("scenario", "steps")
        if scenario == "route":
            return [dist_auth_tag]
        if scenario == "skip_then_drop":
            return [dist_auth_tag, dist_auth_skip_odd, dist_auth_drop_first_job]
        return [dist_auth_tag, dist_auth_each, dist_auth_fragile]

    def define_destination(self, records: Records, runtime_params: AuthoringParams) -> Any:
        if runtime_params.get("scenario") == "route":
            return dist_auth_by_parity
        return api_destination(
            url=f"{_sink(runtime_params)}/ingest/{runtime_params['run']}",
            body={
                "records": [{"id": r["id"]} for r in records],
                "tags": sorted({r["tag"] for r in records}),
            },
        )


def expected_steps_ids(total: int = 20) -> List[int]:
    """What the ``steps`` scenario delivers (mirrors ``dist_auth_each``/``_fragile``)."""
    out: List[int] = []
    for i in range(1, total + 1):
        if i % 5 == 0 or i % 3 == 0 or i == 1:
            continue
        out.append(i)
        if i % 7 == 0:
            out.append(i + 1000)
    return sorted(out)


EXPECTED_STEPS_SKIPS: Dict[str, int] = {"multiple of five": 4, "KeyError": 1}
