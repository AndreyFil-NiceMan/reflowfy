"""Reserved RuntimeParams keys are all optional: subscripting one is an error."""

from typing import Optional

from reflowfy import RuntimeParams


def execution_of(runtime_params: RuntimeParams) -> Optional[str]:
    return runtime_params["execution_id"]  # expect: reportTypedDictNotRequiredAccess
