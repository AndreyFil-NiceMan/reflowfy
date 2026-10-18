"""Session settings for every Reflowfy connection to PostgreSQL (no import side effects).

A process killed without a clean shutdown (SIGKILL, OOM, a lost node) never closes
its database connections. The server only notices when TCP keepalives fail, and its
default is two hours; until then the dead session's locks stay — a manager killed
mid-plan left a session ``idle in transaction`` that blocked the replacement's
startup migrations for 10+ minutes. These settings make the *server* probe the
client, so a dead peer is dropped in about ``idle + interval * count`` seconds.
"""

import os
from typing import Dict


def _seconds(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _enabled(url: str) -> bool:
    """Only for PostgreSQL URLs. DB_SESSION_KEEPALIVES=false turns it off, e.g. behind
    PgBouncer, which rejects startup ``options``."""
    if not url.startswith("postgres"):
        return False
    return os.getenv("DB_SESSION_KEEPALIVES", "true").strip().lower() not in ("0", "false", "no")


def keepalive_settings(url: str) -> Dict[str, str]:
    """``tcp_keepalives_*`` server settings, as strings (idle 30s, 10s x 3 probes ≈ 60s)."""
    if not _enabled(url):
        return {}
    return {
        "tcp_keepalives_idle": str(_seconds("DB_KEEPALIVE_IDLE_SECONDS", 30)),
        "tcp_keepalives_interval": str(_seconds("DB_KEEPALIVE_INTERVAL_SECONDS", 10)),
        "tcp_keepalives_count": str(_seconds("DB_KEEPALIVE_COUNT", 3)),
    }


def sync_connect_args(url: str) -> Dict[str, object]:
    """``connect_args`` for a psycopg2 engine on ``url``."""
    if not _enabled(url):
        return {}
    options = " ".join(f"-c {key}={value}" for key, value in keepalive_settings(url).items())
    return {
        "options": options,
        # client-side probes too, so this process also notices a vanished server
        "keepalives": 1,
        "keepalives_idle": _seconds("DB_KEEPALIVE_IDLE_SECONDS", 30),
        "keepalives_interval": _seconds("DB_KEEPALIVE_INTERVAL_SECONDS", 10),
        "keepalives_count": _seconds("DB_KEEPALIVE_COUNT", 3),
    }


def async_connect_args(url: str) -> Dict[str, object]:
    """``connect_args`` for an asyncpg engine on ``url``."""
    settings = keepalive_settings(url)
    return {"server_settings": settings} if settings else {}
