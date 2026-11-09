"""Secret references and redaction of credentials in job payloads.

Two separate jobs:

* :func:`secret` names an environment variable instead of embedding its value in a
  source's config. The value is read from the environment of the process that builds
  the source, and only the *name* is serialized into the job message
  (``{"$secret": "NAME"}``); the worker reads the same variable from its own
  environment when it rebuilds the source (:func:`decode_secret_refs`).
* :func:`redact_config` / :func:`redact_job_payload` hide credentials in what the API
  returns. A job row stores the whole source descriptor, so without them
  ``GET /executions/{id}/jobs`` handed out database passwords and API tokens.
"""

import os
import re
from typing import Any, Collection, Dict, List, Mapping, Tuple, cast
from urllib.parse import urlsplit, urlunsplit

#: Key of the wire form of a secret reference: ``{"$secret": "ENV_NAME"}``.
SECRET_MARKER = "$secret"

#: What a redacted value is replaced with.
REDACTED = "***"

# Config / params keys whose value is a credential whatever the source declares. Matched
# anywhere in the key (``auth_token``, ``aws_secret_access_key``, ``sasl_password``,
# ``X-Api-Key``), case-insensitively.
_SENSITIVE_KEY = re.compile(
    r"password|passwd|pwd|secret|token|api[-_]?key|authorization|credential|cookie"
    r"|private[-_]?key",
    re.IGNORECASE,
)
# Exact names that are too short to match as a substring safely.
_SENSITIVE_EXACT = frozenset({"auth", "basic_auth", "http_auth"})

# ``scheme://user:password@host`` — only the password is hidden.
_URL_WITH_PASSWORD = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://[^/@\s]*:[^/@\s]*@")


class SecretError(KeyError):
    """A :func:`secret` names an environment variable that is not set."""

    def __str__(self) -> str:  # KeyError would repr() the message
        return str(self.args[0])


class SecretRef(str):
    """A string whose value came from an environment variable.

    It *is* the value (a ``str`` subclass), so every source uses it as it would any
    string, but it remembers the variable's name: serialization writes the name, never
    the value, and ``repr`` shows the name only.
    """

    env_name: str

    def __new__(cls, env_name: str, value: str) -> "SecretRef":
        obj = super().__new__(cls, value)
        obj.env_name = env_name
        return obj

    def __getnewargs__(self) -> Tuple[str, str]:  # type: ignore[override]  # copy/pickle
        return (self.env_name, str(self))

    def __repr__(self) -> str:
        return f"secret({self.env_name!r})"


def secret(env_name: str) -> SecretRef:
    """Reference the credential in environment variable ``env_name``.

    Use it in a source's config instead of the literal value::

        sql_source(connection_url=secret("ORDERS_DB_URL"), query="SELECT ...")

    The job message carries only the name; each process (the manager while it plans,
    every worker while it fetches) reads the variable from its own environment, so the
    variable must be set on all of them. Raises :class:`SecretError` when it is unset.
    There is no default value: it would not travel with the name, so a worker without
    the variable would silently use something else.
    """
    value = os.environ.get(env_name)
    if value is None:
        raise SecretError(
            f"secret({env_name!r}): environment variable {env_name} is not set "
            "in this process (the manager plans with it and every worker fetches "
            "with it, so set it on both)"
        )
    return SecretRef(env_name, value)


def encode_secret_refs(value: Any) -> Any:
    """Copy of ``value`` with every :class:`SecretRef` replaced by its wire form.

    Tuples become lists, as JSON would make them anyway.
    """
    if isinstance(value, SecretRef):
        return {SECRET_MARKER: value.env_name}
    if isinstance(value, dict):
        mapping = cast(Dict[Any, Any], value)
        return {k: encode_secret_refs(v) for k, v in mapping.items()}
    if isinstance(value, (list, tuple)):
        items = cast(List[Any], value)
        return [encode_secret_refs(v) for v in items]
    return value


def _is_marker(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    mapping = cast(Dict[Any, Any], value)
    return len(mapping) == 1 and isinstance(mapping.get(SECRET_MARKER), str)


def decode_secret_refs(value: Any) -> Any:
    """Inverse of :func:`encode_secret_refs`: resolve each reference from the environment."""
    if _is_marker(value):
        return secret(cast(Dict[str, str], value)[SECRET_MARKER])
    if isinstance(value, dict):
        mapping = cast(Dict[Any, Any], value)
        return {k: decode_secret_refs(v) for k, v in mapping.items()}
    if isinstance(value, list):
        items = cast(List[Any], value)
        return [decode_secret_refs(v) for v in items]
    return value


def is_sensitive_key(key: Any) -> bool:
    """True when a config/params key names a credential (``password``, ``auth_token``…)."""
    if not isinstance(key, str):
        return False
    return key.lower() in _SENSITIVE_EXACT or _SENSITIVE_KEY.search(key) is not None


def mask_url_password(value: str) -> str:
    """``postgresql://u:pw@h/db`` → ``postgresql://u:***@h/db``; other strings unchanged."""
    if not _URL_WITH_PASSWORD.match(value):
        return value
    parts = urlsplit(value)
    userinfo, _, hostinfo = parts.netloc.rpartition("@")
    user = userinfo.split(":", 1)[0]
    return urlunsplit(parts._replace(netloc=f"{user}:{REDACTED}@{hostinfo}"))


def _redact_secret_value(value: Any) -> Any:
    """Redact a value that is known to hold a credential."""
    if value is None or value == "" or _is_marker(value) or isinstance(value, SecretRef):
        return encode_secret_refs(value)
    if isinstance(value, str):
        masked = mask_url_password(value)
        if masked != value or "://" in value:
            return masked  # a URL: only its password is secret
        return REDACTED
    if isinstance(value, dict):
        # A mapping (headers, params): only its sensitive keys are secret.
        return _redact(value, ())
    if isinstance(value, (list, tuple)):
        items: List[Any] = list(cast(Any, value))
        if len(items) == 2 and isinstance(items[0], str):  # (username, password)
            return [items[0], _redact_secret_value(items[1])]
        return [_redact_secret_value(v) for v in items]
    return REDACTED


def _redact(value: Any, secret_fields: Collection[str]) -> Any:
    if _is_marker(value) or isinstance(value, SecretRef):
        return encode_secret_refs(value)
    if isinstance(value, dict):
        mapping = cast(Dict[Any, Any], value)
        return {
            k: (
                _redact_secret_value(v)
                if k in secret_fields or is_sensitive_key(k)
                else _redact(v, ())
            )
            for k, v in mapping.items()
        }
    if isinstance(value, (list, tuple)):
        items = cast(List[Any], value)
        return [_redact(v, ()) for v in items]
    if isinstance(value, str):
        return mask_url_password(value)
    return value


def redact_config(config: Any, secret_fields: Collection[str] = ()) -> Any:
    """Copy of ``config`` with credentials hidden.

    Hidden: the top-level keys named in ``secret_fields``, any key at any depth whose
    name looks like a credential (:func:`is_sensitive_key`), and the password of any
    ``scheme://user:password@host`` string. A :func:`secret` reference is shown as its
    variable name: that is not a secret.
    """
    return _redact(config, secret_fields)


def literal_secret_fields(config: Mapping[str, Any], secret_fields: Collection[str]) -> List[str]:
    """Top-level keys of an *encoded* config that hold a literal credential.

    A key counts when redaction would change it: a declared or credential-named field
    with a literal value, or a URL with an inline password. ``secret()`` references
    are already encoded as markers and do not count.
    """
    return [
        key
        for key, value in config.items()
        if _redact({key: value}, secret_fields) != {key: encode_secret_refs(value)}
    ]


def redact_job_payload(payload: Any) -> Any:
    """Copy of a job payload with credentials hidden.

    A v2 job message's ``source`` descriptor is redacted with its source class's
    ``secret_fields``; everything else (``metadata.runtime_params``, or the runtime
    params a DLQ job stores as its payload) by key name and URL passwords.
    """
    if not isinstance(payload, dict):
        return redact_config(payload)
    from reflowfy.factories.source_factory import SourceFactory  # import cycle

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
