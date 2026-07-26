"""Unit tests for the token-bucket rate limiter (jobs-per-minute semantics)."""

import time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from reflowfy.reflow_manager.models import Base, RateLimitState
from reflowfy.reflow_manager.rate_limiter import RateLimiter


@pytest.fixture
def db() -> Session:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def test_rate_limit_is_jobs_per_minute(db: Session) -> None:
    """rate_limit=120 means 120 jobs/minute → 2 tokens/second in the bucket."""
    limiter = RateLimiter(db)

    assert limiter.consume_tokens("p", 1, rate_limit=120) is True

    state = db.query(RateLimitState).filter(RateLimitState.pipeline_name == "p").one()
    assert state.refill_rate == 2.0

    # Bucket is empty right after the first job: at 120/min the next token
    # takes ~0.5 s. Under the old per-second reading it would be instant.
    assert limiter.consume_tokens("p", 1, rate_limit=120) is False

    time.sleep(0.6)
    assert limiter.consume_tokens("p", 1, rate_limit=120) is True


def test_acquire_token_waits_one_minute_slice(db: Session) -> None:
    """acquire_token paces at 60/rate seconds; 600/min ≈ 0.1 s per token."""
    limiter = RateLimiter(db)

    start = time.monotonic()
    for _ in range(3):
        assert limiter.acquire_token("q", rate_limit=600, max_wait=10.0) is True
    elapsed = time.monotonic() - start

    # 3 tokens at 10/s: first is free (bucket starts with 1), then ~0.1 s each.
    assert 0.1 <= elapsed < 3.0
