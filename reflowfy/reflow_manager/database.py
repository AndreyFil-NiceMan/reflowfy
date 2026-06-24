"""Database connection and session management."""

import os
import time
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from reflowfy.reflow_manager.models import Base

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
                print(
                    f"  DB init attempt {attempt}/{max_retries} failed ({exc}), retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)
    raise RuntimeError(
        f"Database init failed after {max_retries} attempts: {last_err}"
    ) from last_err


def _apply_column_migrations() -> None:
    """Add new columns to existing tables without dropping data."""
    from sqlalchemy import text

    migrations = [
        # P1.1 — store full worker traceback alongside the error summary
        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS error_traceback TEXT",
        # P1.4 — track how many jobs were skipped by content-hash deduplication
        "ALTER TABLE executions ADD COLUMN IF NOT EXISTS deduplicated_jobs INTEGER DEFAULT 0",
    ]

    with engine.begin() as conn:
        for stmt in migrations:
            conn.execute(text(stmt))


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
