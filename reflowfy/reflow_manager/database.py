"""Database connection and session management."""

import logging
import os
import re
import time
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from reflowfy.reflow_manager.db_settings import sync_connect_args
from reflowfy.reflow_manager.models import Base

logger = logging.getLogger(__name__)


# Database URL from environment
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy")

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,  # Verify connections before using
    echo=False,  # Set to True for SQL query logging
    # Make the server drop this process's sessions soon after it dies (see db_settings).
    connect_args=sync_connect_args(DATABASE_URL),
)

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(max_retries: int = 10, retry_delay: float = 2.0) -> None:
    """
    Initialize database tables and apply incremental column migrations.

    Retries the connection up to `max_retries` times to handle the race
    condition where the ReflowManager starts before PostgreSQL is ready.
    """
    last_err: Exception = RuntimeError("no attempts made")
    for attempt in range(1, max_retries + 1):
        try:
            Base.metadata.create_all(bind=engine)
            _apply_column_migrations()
            return
        except Exception as exc:
            last_err = exc
            if attempt < max_retries:
                logger.warning(
                    "DB init attempt %d/%d failed (%s), retrying in %ss",
                    attempt,
                    max_retries,
                    exc,
                    retry_delay,
                    extra={"event": "db_connect_retry"},
                )
                time.sleep(retry_delay)
    raise RuntimeError(
        f"Database init failed after {max_retries} attempts: {last_err}"
    ) from last_err


# How long one migration statement may wait for a table lock before giving up.
MIGRATION_LOCK_TIMEOUT = "5s"
_ADD_COLUMN = re.compile(r"ALTER TABLE (\w+) ADD COLUMN IF NOT EXISTS (\w+)", re.IGNORECASE)


def _apply_column_migrations() -> None:
    """Add new columns to existing tables without dropping data.

    Startup must never queue behind another session. ``ALTER TABLE`` takes an exclusive
    lock even when ``IF NOT EXISTS`` finds nothing to do, so one session left open by a
    killed process used to hang every restart — the replacement of a manager killed
    mid-plan sat at "Waiting for application startup" for 10+ minutes. So a column that
    already exists is skipped without touching the table, and every statement that does
    run gives up after ``MIGRATION_LOCK_TIMEOUT`` instead of waiting.
    """
    from sqlalchemy import inspect, text
    from sqlalchemy.exc import OperationalError

    migrations = [
        # P1.1 — store full worker traceback alongside the error summary
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS error_traceback TEXT",
        # P1.4 — track how many jobs were skipped by content-hash deduplication
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS deduplicated_jobs INTEGER DEFAULT 0",
        # Persist the /run rate_limit override so resume_execution can reuse it
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS rate_limit DOUBLE PRECISION",
        # Worker claims per job; bounds re-dispatch of jobs whose worker was lost
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0",
        # Dispatch-loop ownership lease (one loop per execution, takeover when stale)
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS driver_id VARCHAR(255)",
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS driver_heartbeat_at TIMESTAMP",
        # Planning finished (NULL = an execution from before this column: treat as planned)
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS plan_complete BOOLEAN",
        # Retention sweeper filters on completed_at; partial index skips unfinished rows
        "CREATE INDEX IF NOT EXISTS idx_executions_completed_at "
        "ON executions(completed_at) WHERE completed_at IS NOT NULL",
        # Bulk retention deletes leave dead tuples; vacuum at 2% instead of the 20% default
        "ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02, "
        "autovacuum_analyze_scale_factor = 0.02)",
        "ALTER TABLE executions SET (autovacuum_vacuum_scale_factor = 0.02, "
        "autovacuum_analyze_scale_factor = 0.02)",
        # One row per deduplicated record, swept after the retention window
        "ALTER TABLE processed_records SET (autovacuum_vacuum_scale_factor = 0.02, "
        "autovacuum_analyze_scale_factor = 0.02)",
    ]

    inspector = inspect(engine)
    existing = {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in ("jobs", "executions")
    }
    for stmt in migrations:
        add_column = _ADD_COLUMN.match(stmt)
        if add_column and add_column.group(2) in existing.get(add_column.group(1), set()):
            continue  # already there: no lock needed, and the usual case on every restart
        try:
            with engine.begin() as conn:
                conn.execute(text(f"SET LOCAL lock_timeout = '{MIGRATION_LOCK_TIMEOUT}'"))
                conn.execute(text(stmt))
        except OperationalError:
            if add_column:
                raise  # a column the code needs is missing: init_db retries, then fails loudly
            # index / vacuum settings only tune performance; they run on a later restart
            logger.warning(
                "Skipping optional migration (table locked): %s",
                stmt,
                exc_info=True,
                extra={"event": "migration_skipped"},
            )


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session.

    Yields:
        Database session

    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            # Use db session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def close_db() -> None:
    """Close database engine."""
    engine.dispose()
