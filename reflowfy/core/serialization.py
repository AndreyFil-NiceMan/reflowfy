"""JSON-safe normalization for records and payloads."""

from typing import Any


def to_json_safe(obj: Any) -> Any:
    """Recursively convert an object to a JSON-serializable form.

    Mirrors the normalization the manager historically applied to job
    payloads so that non-JSON-native values (e.g. ``datetime``) become
    strings before reaching a destination's ``json.dumps``.
    """
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, dict):
        return {k: to_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_json_safe(item) for item in obj]
    elif hasattr(obj, "to_dict"):
        return to_json_safe(obj.to_dict())
    elif hasattr(obj, "__dict__"):
        return to_json_safe(obj.__dict__)
    else:
        return str(obj)
