"""Optional bearer-token authentication for the API and ReflowManager services.

Set ``REFLOWFY_API_TOKEN`` and every route of both services except ``/health`` and
``/metrics`` (probes and Prometheus) requires ``Authorization: Bearer <token>``. Unset
or empty, nothing changes. The API forwards the same token to the manager
(:func:`auth_headers`), so both services are given the same value.
"""

import hmac
import json
import logging
import os
from typing import Any, Awaitable, Callable, Dict, MutableMapping, Optional

logger = logging.getLogger(__name__)

API_TOKEN_ENV = "REFLOWFY_API_TOKEN"

#: Paths served without a token: liveness/readiness probes and the metrics scrape.
OPEN_PATHS = frozenset({"/health", "/metrics"})

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


def api_token() -> Optional[str]:
    """The configured token, or ``None`` when authentication is off."""
    token = os.environ.get(API_TOKEN_ENV, "").strip()
    return token or None


def auth_headers() -> Dict[str, str]:
    """Headers a client of these services sends: the bearer token when one is set."""
    token = api_token()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _presented_token(scope: Scope) -> Optional[str]:
    headers: Any = scope.get("headers") or []
    for name, value in headers:
        if name.lower() == b"authorization":
            scheme, _, credential = bytes(value).decode("latin-1").partition(" ")
            if scheme.lower() == "bearer" and credential.strip():
                return credential.strip()
            return None
    return None


class BearerTokenMiddleware:
    """ASGI middleware enforcing :data:`API_TOKEN_ENV` (read on every request)."""

    def __init__(self, app: ASGIApp, service: str = "api"):
        self.app = app
        self.service = service

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        token = api_token()
        if (
            token is None
            or scope.get("type") != "http"
            or scope.get("method") == "OPTIONS"  # CORS preflight carries no credentials
            or scope.get("path") in OPEN_PATHS
        ):
            await self.app(scope, receive, send)
            return
        presented = _presented_token(scope)
        if presented is not None and hmac.compare_digest(
            presented.encode("utf-8"), token.encode("utf-8")
        ):
            await self.app(scope, receive, send)
            return
        reason = "missing" if presented is None else "invalid"
        # Log the decision, never the token presented.
        logger.info(
            "Rejected %s request to %s: %s bearer token",
            scope.get("method"),
            scope.get("path"),
            reason,
            extra={"event": "auth_rejected", "reason": reason},
        )
        body = json.dumps(
            {"detail": f"{reason} bearer token: send 'Authorization: Bearer <REFLOWFY_API_TOKEN>'"}
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"www-authenticate", b'Bearer realm="reflowfy"'),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


def install_api_auth(app: Any, service: str) -> None:
    """Add :class:`BearerTokenMiddleware` to a FastAPI app and log whether it is on."""
    app.add_middleware(BearerTokenMiddleware, service=service)
    enabled = api_token() is not None
    logger.info(
        "API token authentication %s",
        f"on ({API_TOKEN_ENV} set; /health and /metrics stay open)" if enabled else "off",
        extra={"event": "auth_configured", "enabled": enabled},
    )
