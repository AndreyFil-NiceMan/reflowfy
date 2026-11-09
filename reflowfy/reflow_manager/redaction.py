"""Hide credentials in the job, execution, schedule and DLQ rows the manager's routes return.

The stored rows are unchanged: only what the API hands out is redacted.
"""

from typing import Any, Dict, cast

from reflowfy.core.secrets import redact_config
from reflowfy.factories.source_factory import SourceFactory


def redact_job_payload(payload: Any) -> Any:
    """Copy of a job payload with credentials hidden.

    A v2 job message's ``source`` descriptor is redacted with its source class's
    ``secret_fields``; everything else (``metadata.runtime_params``, or the runtime
    params a DLQ job stores as its payload) by key name and URL passwords.
    """
    if not isinstance(payload, dict):
        return redact_config(payload)
    data = cast(Dict[str, Any], payload)
    redacted: Dict[str, Any] = redact_config({k: v for k, v in data.items() if k != "source"})
    source = data.get("source")
    if isinstance(source, dict):
        redacted["source"] = SourceFactory.redact(cast(Dict[str, Any], source))
    elif "source" in data:
        redacted["source"] = redact_config(source)
    return {k: redacted[k] for k in data}  # keep the key order


#: Fields of an API row (job, execution, schedule, DLQ job) that can hold credentials.
_ROW_FIELDS = ("job_payload", "runtime_params")


def redact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Copy of an API row dict with its ``job_payload`` / ``runtime_params`` redacted.

    Every API response that returns job, execution, schedule or DLQ rows goes through
    this, so a credential stored in a job message or passed as a run parameter is not
    handed out to whoever can read the API.
    """
    redacted = dict(row)
    for field in _ROW_FIELDS:
        if field in redacted:
            redacted[field] = redact_job_payload(redacted[field])
    return redacted
