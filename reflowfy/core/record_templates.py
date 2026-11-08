"""``{field}`` templates filled from a record — one implementation for every destination.

Used by ``kafka_destination`` (``headers=``, ``key=``) and ``api_destination`` (``url``,
``params``, ``headers=``) so that a template means the same thing everywhere:

- ``{field}`` is replaced by ``record["field"]``; ``{data.region}`` walks nested dicts
  (records from a source are ``{"data": {...}}``). A key that itself contains a dot
  is matched first, so ``{a.b}`` still finds ``record["a.b"]``.
- Only ``{`` + a dotted identifier + ``}`` is a placeholder: other braces (JSON in a
  header, say) are kept, and nothing is evaluated (deliberately not ``str.format``,
  which would run ``{a[0]}`` and format specs against record data).
- A placeholder naming a field the record lacks raises :class:`TemplateError` (a
  ``ValueError``, so destinations do not retry it) naming the template and the
  record's position — never the record's values.
"""

import re
from typing import Any, Callable, Dict, Optional, cast

PLACEHOLDER = re.compile(r"\{(\w+(?:\.\w+)*)\}")

_MISSING = object()


class TemplateError(ValueError):
    """A template names a field the record does not have."""


def has_placeholders(template: object) -> bool:
    """Whether ``template`` is a string with at least one ``{field}``."""
    return isinstance(template, str) and PLACEHOLDER.search(template) is not None


def lookup(record: Any, path: str) -> Any:
    """``record[path]``, walking dots through nested dicts; ``_MISSING`` when absent."""
    if not isinstance(record, dict):
        return _MISSING
    fields = cast(Dict[str, Any], record)
    if path in fields:
        return fields[path]
    value: Any = fields
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return _MISSING
        value = cast(Dict[str, Any], value)[part]
    return value


def _missing(owner: str, what: str, field: str, index: int, total: int) -> TemplateError:
    return TemplateError(
        f"{owner}: {what} placeholder {{{field}}} has no matching field on "
        f"record {index + 1} of {total}"
    )


def fill(
    template: str,
    record: Any,
    *,
    owner: str,
    what: str,
    index: int,
    total: int,
    encode: Optional[Callable[[str], str]] = None,
) -> str:
    """``template`` with every ``{field}`` replaced by the record's value, as text.

    ``encode`` is applied to each value (``quote`` for a URL path); the template's own
    text is left as written.
    """
    if "{" not in template:
        return template

    def substitute(match: "re.Match[str]") -> str:
        value = lookup(record, match.group(1))
        if value is _MISSING:
            raise _missing(owner, what, match.group(1), index, total)
        text = str(value)
        return encode(text) if encode is not None else text

    return PLACEHOLDER.sub(substitute, template)


def fill_value(value: Any, record: Any, *, owner: str, what: str, index: int, total: int) -> Any:
    """Like :func:`fill`, but ``"{field}"`` alone keeps the value's own type.

    For query parameters: an int stays an int, a list stays a list (httpx repeats
    the parameter). Anything that is not a string is returned unchanged.
    """
    if not isinstance(value, str) or "{" not in value:
        return value
    whole = PLACEHOLDER.fullmatch(value)
    if whole:
        found = lookup(record, whole.group(1))
        if found is _MISSING:
            raise _missing(owner, what, whole.group(1), index, total)
        return found
    return fill(value, record, owner=owner, what=what, index=index, total=total)
