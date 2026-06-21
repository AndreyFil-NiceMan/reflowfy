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

    Unknown/``None`` ``auth_type`` or a falsy ``auth_token`` leaves the headers
    unchanged. The input dict is never mutated.
    """
    result = dict(headers)
    if not auth_token:
        return result
    if auth_type == "bearer":
        result["Authorization"] = f"Bearer {auth_token}"
    elif auth_type == "apikey":
        result["X-API-Key"] = auth_token
    elif auth_type == "basic":
        encoded = base64.b64encode(auth_token.encode("utf-8")).decode("ascii")
        result["Authorization"] = f"Basic {encoded}"
    return result
