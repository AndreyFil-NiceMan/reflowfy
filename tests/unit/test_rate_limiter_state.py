"""Token-bucket state of the RateLimiter, on a frozen clock.

``test_rate_limiter.py`` checks the jobs-per-minute unit with real sleeps; these tests
pin the bucket mechanics themselves: the row created on first use, refill over elapsed
time, the burst cap, a rate change, ``acquire_token``'s timeout, and two managers
sharing one bucket through the database.
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

import reflowfy.reflow_manager.rate_limiter as rl_mod
from reflowfy.reflow_manager.models import Base, RateLimitState
from reflowfy.reflow_manager.rate_limiter import RateLimiter

T0 = datetime(2026, 10, 20, 12, 0, 0)


class _Clock:
    """Drives both ``datetime.now`` and ``time.time``/``time.sleep`` in rate_limiter."""

    def __init__(self) -> None:
        self.now = T0
        self.sleeps: List[float] = []

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    # time module
    def time(self) -> float:
        return (self.now - T0).total_seconds()

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.advance(seconds)


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    c = _Clock()

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[override]
            return c.now.replace(tzinfo=tz)

    monkeypatch.setattr(rl_mod, "datetime", _FrozenDatetime)
    monkeypatch.setattr(rl_mod, "time", SimpleNamespace(time=c.time, sleep=c.sleep))
    return c


@pytest.fixture
def engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path}/rl.db")
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine) -> Session:
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _state(db: Session, name: str = "p") -> RateLimitState:
    db.expire_all()
    return db.query(RateLimitState).filter(RateLimitState.pipeline_name == name).one()


def test_state_is_created_on_first_use_with_one_token(db, clock):
    assert db.query(RateLimitState).count() == 0

    assert RateLimiter(db).can_dispatch("p", count=1, rate_limit=120) is True

    state = _state(db)
    assert state.tokens == 1.0  # no initial burst
    assert state.refill_rate == 2.0  # 120/min
    assert state.max_tokens == 2.0  # one second's worth
    assert state.last_update == T0


def test_can_dispatch_does_not_consume(db, clock):
    limiter = RateLimiter(db)
    assert limiter.can_dispatch("p", count=2, rate_limit=120) is False
    assert limiter.can_dispatch("p", count=1, rate_limit=120) is True
    assert limiter.can_dispatch("p", count=1, rate_limit=120) is True
    assert _state(db).tokens == 1.0


def test_a_slow_rate_still_holds_one_whole_token(db, clock):
    RateLimiter(db).can_dispatch("p", rate_limit=6)  # 0.1 token/s
    state = _state(db)
    assert state.refill_rate == pytest.approx(0.1)
    assert state.max_tokens == 1.0  # never below one job, or nothing could ever pass


def test_the_default_rate_applies_without_an_override(db, clock):
    RateLimiter(db, default_rate=300).can_dispatch("p")
    assert _state(db).refill_rate == 5.0


def test_tokens_refill_with_elapsed_time(db, clock):
    limiter = RateLimiter(db)
    assert limiter.consume_tokens("p", rate_limit=120) is True
    assert _state(db).tokens == 0.0

    clock.advance(0.25)  # half a token at 2/s
    assert limiter.consume_tokens("p", rate_limit=120) is False
    state = _state(db)
    assert (state.tokens, state.last_update) == (0.0, T0)  # a refusal changes nothing

    clock.advance(0.25)
    assert limiter.consume_tokens("p", rate_limit=120) is True
    state = _state(db)
    assert state.tokens == pytest.approx(0.0)
    assert state.last_update == T0 + timedelta(seconds=0.5)


def test_refill_through_can_dispatch_is_persisted(db, clock):
    limiter = RateLimiter(db)
    limiter.consume_tokens("p", rate_limit=120)
    clock.advance(0.5)

    assert limiter.can_dispatch("p", rate_limit=120) is True

    state = _state(db)
    assert state.tokens == pytest.approx(1.0)
    assert state.last_update == T0 + timedelta(seconds=0.5)


def test_an_idle_bucket_is_capped_at_its_burst_size(db, clock):
    limiter = RateLimiter(db)
    limiter.consume_tokens("p", rate_limit=600)  # 10 tokens/s, burst of 10
    clock.advance(3600)  # an hour idle would be 36,000 tokens uncapped

    assert limiter.can_dispatch("p", count=11, rate_limit=600) is False
    assert _state(db).tokens == 10.0
    for _ in range(10):
        assert limiter.consume_tokens("p", rate_limit=600) is True
    assert limiter.consume_tokens("p", rate_limit=600) is False


def test_a_changed_rate_updates_the_stored_bucket(db, clock):
    limiter = RateLimiter(db)
    limiter.can_dispatch("p", rate_limit=120)

    limiter.can_dispatch("p", rate_limit=600)

    state = _state(db)
    assert (state.refill_rate, state.max_tokens) == (10.0, 10.0)
    assert state.tokens == 1.0  # tokens on hand are kept


def test_buckets_are_per_key(db, clock):
    limiter = RateLimiter(db)
    assert limiter.consume_tokens("p:e1", rate_limit=60) is True
    assert limiter.consume_tokens("p:e2", rate_limit=60) is True  # its own bucket
    assert limiter.consume_tokens("p:e1", rate_limit=60) is False


def test_two_managers_share_one_bucket(engine, clock):
    """Two manager pods = two sessions; the bucket lives in the database, not in memory."""
    with sessionmaker(bind=engine)() as a_db, sessionmaker(bind=engine)() as b_db:
        a, b = RateLimiter(a_db), RateLimiter(b_db)

        assert a.consume_tokens("p", rate_limit=120) is True
        assert b.consume_tokens("p", rate_limit=120) is False  # A took the only token

        clock.advance(0.5)
        assert b.consume_tokens("p", rate_limit=120) is True
        assert a.consume_tokens("p", rate_limit=120) is False  # B took the refill

        clock.advance(1.0)  # two tokens: one each
        assert a.consume_tokens("p", rate_limit=120) is True
        assert b.consume_tokens("p", rate_limit=120) is True
        assert a.consume_tokens("p", rate_limit=120) is False

        assert a_db.query(RateLimitState).count() == 1


def test_acquire_token_waits_one_token_interval(db, clock):
    limiter = RateLimiter(db)
    assert limiter.acquire_token("p", rate_limit=60) is True  # the starting token
    assert clock.sleeps == []

    assert limiter.acquire_token("p", rate_limit=60) is True
    assert clock.sleeps == [1.0]  # 60/min: one second per job


def test_acquire_token_gives_up_at_max_wait(db, clock):
    limiter = RateLimiter(db)
    assert limiter.acquire_token("p", rate_limit=1) is True  # one job per minute

    assert limiter.acquire_token("p", rate_limit=1, max_wait=25.0) is False
    assert clock.sleeps == [25.0]  # capped by what is left of max_wait, not 60 s
    assert _state(db).tokens == pytest.approx(0.0)
