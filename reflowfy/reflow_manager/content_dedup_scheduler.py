"""Background sweeper that purges expired processed_content rows.

Mirrors pipeline_scheduler.py: a daemon thread polling at an interval.
Retention defaults to 24h; the same window bounds the rare case of a
worker that crashed between claiming a hash and finishing the send.
"""

import logging
import os
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session

from reflowfy.reflow_manager.models import ProcessedContent
from reflowfy.reflow_manager.database import SessionLocal

logger = logging.getLogger(__name__)

CONTENT_DEDUP_RETENTION_HOURS = int(os.getenv("CONTENT_DEDUP_RETENTION_HOURS", "24"))
CONTENT_DEDUP_SWEEP_INTERVAL = int(os.getenv("CONTENT_DEDUP_SWEEP_INTERVAL_SECONDS", "3600"))


def purge_expired_content(db: Session, retention_hours: int, now: Optional[datetime] = None) -> int:
    """Delete processed_content rows older than retention_hours. Returns count."""
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(hours=retention_hours)
    result = db.execute(delete(ProcessedContent).where(ProcessedContent.created_at < cutoff))
    return getattr(result, "rowcount", 0) or 0


class ContentDedupScheduler:
    """Daemon thread that periodically purges expired content hashes."""

    def __init__(
        self,
        retention_hours: int = CONTENT_DEDUP_RETENTION_HOURS,
        sweep_interval: int = CONTENT_DEDUP_SWEEP_INTERVAL,
    ):
        self.retention_hours = retention_hours
        self.sweep_interval = sweep_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Content Dedup Sweeper started (every %ss, retain %sh)",
            self.sweep_interval,
            self.retention_hours,
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
            self._thread = None

    def _run_loop(self) -> None:
        while self._running:
            db = SessionLocal()
            try:
                deleted = purge_expired_content(db, self.retention_hours)
                db.commit()
                if deleted:
                    logger.info("Content Dedup Sweeper purged %d expired hash(es)", deleted)
            except Exception:  # pragma: no cover
                db.rollback()
                logger.error("Content Dedup Sweeper error", exc_info=True)
            finally:
                db.close()
            self._stop_event.wait(timeout=self.sweep_interval)


_sweeper: Optional[ContentDedupScheduler] = None


def init_content_dedup_scheduler() -> ContentDedupScheduler:
    global _sweeper
    _sweeper = ContentDedupScheduler()
    _sweeper.start()
    return _sweeper


def stop_content_dedup_scheduler() -> None:
    global _sweeper
    if _sweeper:
        _sweeper.stop()
        _sweeper = None
