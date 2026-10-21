"""A misspelled params key inside a typed @transformation body is caught."""

from typing import Literal

from typing_extensions import Annotated, NotRequired

from reflowfy import Param, Records, RuntimeParams, transformation


class StatusParams(RuntimeParams, total=False):
    filter_status: Annotated[NotRequired[Literal["active", "inactive"]], Param(default="active")]


@transformation("typecheck_fail_misspelled")
def filter_status(records: Records, runtime_params: StatusParams) -> Records:
    if runtime_params["filter_staus"]:  # expect: reportGeneralTypeIssues
        return []
    return records
