"""Why a connector's health probe failed.

A probe returns ``False`` rather than raising, and the caller only logs that the
check failed; without this the cause (refused connection, bad credentials, DNS) was
gone. The message has URL passwords masked: a driver error often quotes the
connection URL.
"""

from typing import Any

from reflowfy.core.secrets import mask_url_passwords_in_text


def log_unhealthy(log: Any, what: str, exc: BaseException) -> bool:
    """Log ``health_check_failed`` for ``what`` (e.g. "SQL source"); returns False."""
    log.warning(
        "%s health check failed: %s: %s",
        what,
        type(exc).__name__,
        mask_url_passwords_in_text(str(exc)),
        extra={"event": "health_check_failed", "error_type": type(exc).__name__},
    )
    return False


__all__ = ["log_unhealthy"]
