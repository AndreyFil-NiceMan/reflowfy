"""A @transformation annotated with the pipeline's own params TypedDict is accepted."""

from typing import Literal

from typing_extensions import Annotated, NotRequired

from reflowfy import Param, Records, RuntimeParams, transformation


class StatusParams(RuntimeParams, total=False):
    filter_status: Annotated[NotRequired[Literal["active", "inactive"]], Param(default="active")]


@transformation("typecheck_pass_filter")
def filter_status(records: Records, runtime_params: StatusParams) -> Records:
    wanted = runtime_params.get("filter_status", "active")
    return [r for r in records if r.get("status") == wanted]


instance = filter_status()
