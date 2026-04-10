"""Database connection and session management."""

import os
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from reflowfy.reflow_manager.models import Base


# Database URL from environment
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy"
)

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


def init_db() -> None:
    """
    Initialize database tables.

    Creates all tables defined in models if they don't exist.
    """
    Base.metadata.create_all(bind=engine)


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
