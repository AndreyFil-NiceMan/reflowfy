"""Comparing records, and masking fields in them, for fixtures and assertions."""

from typing import Any, Dict, List, Sequence, Tuple, cast

REDACTED = "<redacted>"
_DROP = object()


def _paths(fields: Sequence[str]) -> List[Tuple[str, ...]]:
    return [tuple(part for part in name.split(".") if part) for name in fields if name.strip(".")]


def _replace(value: Any, paths: List[Tuple[str, ...]], trail: Tuple[str, ...], new: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[Any, Any] = {}
        for key, item in cast(Dict[Any, Any], value).items():
            here = trail + (str(key),)
            if any(here[-len(path) :] == path for path in paths):
                if new is not _DROP:
                    out[key] = new
            else:
                out[key] = _replace(item, paths, here, new)
        return out
    if isinstance(value, list):
        # A list adds nothing to the path: "items.price" names the price of every item.
        return [_replace(item, paths, trail, new) for item in cast(List[Any], value)]
    return value


def redact(value: Any, fields: Sequence[str]) -> Any:
    """A copy of ``value`` with the named fields replaced by ``"<redacted>"``.

    A field is a key, or a dotted path of keys, matched wherever it appears:
    ``email`` masks every ``email`` key at any depth, ``customer.email`` only an
    ``email`` directly under a ``customer``. Lists are looked through.
    """
    paths = _paths(fields)
    return _replace(value, paths, (), REDACTED) if paths else value


def without_fields(value: Any, fields: Sequence[str]) -> Any:
    """A copy of ``value`` with the named fields removed (matched as :func:`redact`)."""
    paths = _paths(fields)
    return _replace(value, paths, (), _DROP) if paths else value


def diff_values(expected: Any, actual: Any, path: str = "") -> List[str]:
    """One line per difference between two JSON-like values, naming where it is.

    ``tax: 2.0 → 2.2``, ``data.email: missing (expected 'a@b.c')``,
    ``country: unexpected 'IL'``.
    """
    if isinstance(expected, dict) and isinstance(actual, dict):
        left, right = cast(Dict[Any, Any], expected), cast(Dict[Any, Any], actual)
        lines: List[str] = []
        for key in list(left) + [k for k in right if k not in left]:
            where = f"{path}.{key}" if path else str(key)
            if key not in right:
                lines.append(f"{where}: missing (expected {left[key]!r})")
            elif key not in left:
                lines.append(f"{where}: unexpected {right[key]!r}")
            else:
                lines.extend(diff_values(left[key], right[key], where))
        return lines
    if isinstance(expected, list) and isinstance(actual, list):
        before, after = cast(List[Any], expected), cast(List[Any], actual)
        lines = []
        if len(before) != len(after):
            lines.append(f"{path or 'list'}: {len(before)} item(s) → {len(after)}")
        for index, (old, new) in enumerate(zip(before, after)):
            lines.extend(diff_values(old, new, f"{path}[{index}]"))
        return lines
    # 2 == 2.0 on purpose: they are the same number once they have been through JSON.
    if expected != actual:
        return [f"{path or 'value'}: {expected!r} → {actual!r}"]
    return []


def summarize(lines: List[str], limit: int = 20) -> str:
    """The first ``limit`` difference lines, and how many more there are."""
    shown = lines[:limit]
    if len(lines) > limit:
        shown.append(f"… {len(lines) - limit} more")
    return "\n".join(f"  {line}" for line in shown)
