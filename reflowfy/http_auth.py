"""Shared HTTP authentication header construction.

Used by both the API source (sync ``httpx.Client``) and the API destination
(async ``httpx.AsyncClient``) so auth lives in exactly one place.
"""

from __future__ import annotations

import base64
from typing import Dict, Optional


def build_auth_headers(
    headers: Dict[str, str],
    auth_type: Optional[str],
    auth_token: Optional[str],
) -> Dict[str, str]:
    """Return a new headers dict with the auth header applied.

    - ``bearer``  -> ``Authorization: Bearer <token>``
    - ``apikey``  -> ``X-API-Key: <token>``
    - ``basic``   -> ``Authorization: Basic base64(<token>)`` where ``token``
      is ``"username:password"``.

    ``auth_type`` is case-insensitive. ``None`` or a falsy ``auth_token`` leaves the
    headers unchanged; any other name raises ``ValueError`` (it used to send the
    requests with no auth at all, which surfaced only as a 401 later). The input dict
    is never mutated.
    """
    result = dict(headers)
    if not auth_token or auth_type is None:
        return result
    kind = auth_type.lower()
    if kind == "bearer":
        result["Authorization"] = f"Bearer {auth_token}"
    elif kind == "apikey":
        result["X-API-Key"] = auth_token
    elif kind == "basic":
        encoded = base64.b64encode(auth_token.encode("utf-8")).decode("ascii")
        result["Authorization"] = f"Basic {encoded}"
    else:
        raise ValueError(f"unknown auth_type {auth_type!r}: use 'bearer', 'apikey' or 'basic'")
    return result
