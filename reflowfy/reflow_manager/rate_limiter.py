"""Rate limiter using token bucket algorithm for ReflowManager."""

import time
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from reflowfy.store.models import RateLimitState

SECONDS_PER_MINUTE = 60.0


def execution_bucket_key(pipeline_name: str, execution_id: str) -> str:
    """The token bucket of one execution.

    Per execution, not per pipeline: two executions of one pipeline running at once
    each get their own rate instead of overwriting each other's refill rate.
    """
    return f"{pipeline_name}:{execution_id}"


class RateLimiter:
    """
    Token bucket rate limiter with database-backed state.

    One bucket per key — the drive loop uses one per execution
    (:func:`execution_bucket_key`) — with persistent state that survives restarts
    and works across multiple instances.

    All rate limits in the public API are expressed in **jobs per minute**;
    they are converted to tokens per second for the bucket, which is what
    `rate_limit_state.refill_rate` stores. No rate limit (None) means unlimited:
    no token is taken and no bucket row is written.
    """

    def __init__(self, db_session: Session, default_rate: Optional[float] = None):
        """
        Initialize rate limiter.

        Args:
            db_session: SQLAlchemy database session
            default_rate: Rate (jobs per minute) for a call that passes none; None
                (the default) leaves such calls unlimited.
        """
        self.db = db_session
        self.default_rate = default_rate

    def delete_bucket(self, key: str) -> int:
        """Remove a bucket (its execution ended). Returns the rows deleted (0 or 1)."""
        deleted = (
            self.db.query(RateLimitState)
            .filter(RateLimitState.pipeline_name == key)
            .delete(synchronize_session=False)
        )
        self.db.commit()
        return deleted

    def _tokens_per_second(self, rate_per_minute: float) -> float:
        """Convert a jobs-per-minute rate limit into the bucket's tokens/second."""
        return rate_per_minute / SECONDS_PER_MINUTE

    def _get_or_create_state(self, pipeline_name: str, rate_limit: float) -> RateLimitState:
        """
        Get or create rate limit state for a pipeline, updating rate if different.

        Args:
            pipeline_name: Pipeline name
            rate_limit: Rate limit in jobs per minute
        """
        refill_rate = self._tokens_per_second(rate_limit)
        state = self.db.query(RateLimitState).filter(
            RateLimitState.pipeline_name == pipeline_name
        ).first()

        if not state:
            # Start with 1 token for strict rate limiting from the beginning
            # (not full bucket which would allow initial burst)
            state = RateLimitState(
                pipeline_name=pipeline_name,
                tokens=1.0,  # Start with minimal tokens - no burst allowed
                max_tokens=max(1.0, refill_rate),
                refill_rate=refill_rate,
                last_update=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            self.db.add(state)
            self.db.commit()
            self.db.refresh(state)
        elif state.refill_rate != refill_rate:
            # Update the rate limit if it's different from what's stored
            # This handles rate_limit overrides from API calls
            state.refill_rate = refill_rate
            state.max_tokens = max(1.0, refill_rate)
            self.db.commit()

        return state

    def _refill_tokens(self, state: RateLimitState) -> None:
        """Refill tokens based on elapsed time."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        elapsed = (now - state.last_update).total_seconds()

        # Add tokens based on refill rate
        new_tokens = state.tokens + (elapsed * state.refill_rate)
        state.tokens = min(new_tokens, state.max_tokens)
        state.last_update = now

        self.db.commit()

    def can_dispatch(
        self,
        pipeline_name: str,
        count: int = 1,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """
        Check if we can dispatch jobs based on rate limit.

        Args:
            pipeline_name: Pipeline name
            count: Number of jobs to dispatch
            rate_limit: Optional override rate limit (jobs per minute)

        Returns:
            True if dispatch is allowed, False otherwise
        """
        effective_rate = rate_limit if rate_limit is not None else self.default_rate
        if effective_rate is None:
            return True  # unlimited
        state = self._get_or_create_state(pipeline_name, effective_rate)

        self._refill_tokens(state)

        return state.tokens >= count

    def _get_state_with_lock(
        self,
        pipeline_name: str,
        rate_limit: float,
    ) -> RateLimitState:
        """
        Get rate limit state with row-level lock for atomic updates.

        Uses SELECT FOR UPDATE to prevent race conditions when multiple
        RM pods try to consume tokens simultaneously.

        Args:
            pipeline_name: Pipeline name
            rate_limit: The rate limit to use (jobs per minute)

        Returns:
            Locked RateLimitState object
        """
        # First ensure the state exists (without lock)
        self._get_or_create_state(pipeline_name, rate_limit)

        # Now get it with a lock for atomic update
        locked_state = self.db.query(RateLimitState).filter(
            RateLimitState.pipeline_name == pipeline_name
        ).with_for_update().first()

        return locked_state

    def consume_tokens(
        self,
        pipeline_name: str,
        count: int = 1,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """
        Consume tokens for rate limiting.

        Uses row-level locking to prevent race conditions when multiple
        RM pods try to consume tokens concurrently.

        Args:
            pipeline_name: Pipeline name
            count: Number of tokens to consume
            rate_limit: Optional override rate limit (jobs per minute)

        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        effective_rate = rate_limit if rate_limit is not None else self.default_rate
        if effective_rate is None:
            return True  # unlimited

        # Get state with row-level lock to prevent concurrent consumption
        state = self._get_state_with_lock(pipeline_name, effective_rate)

        # Calculate how many tokens should have been added
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        elapsed = (now - state.last_update).total_seconds()
        new_tokens = state.tokens + (elapsed * state.refill_rate)
        new_tokens = min(new_tokens, state.max_tokens)

        if new_tokens >= count:
            # Only update state when we actually consume a token
            state.tokens = new_tokens - count
            state.last_update = now
            self.db.commit()
            return True

        # Release the lock without changes
        self.db.rollback()
        return False

    def acquire_token(
        self,
        pipeline_name: str,
        rate_limit: Optional[float] = None,
        max_wait: float = 300.0,
    ) -> bool:
        """
        Atomically acquire a token, waiting if necessary.

        This method will wait until a token becomes available or max_wait expires.
        With proper rate limiting, it will block for 60/rate_limit seconds between tokens.

        Args:
            pipeline_name: Pipeline name
            rate_limit: Optional override rate limit (jobs per minute)
            max_wait: Maximum time to wait for a token (seconds). Default 300s,
                which covers the slowest useful rate of 1 job/minute.

        Returns:
            True if token was acquired, False if timed out
        """
        start_time = time.time()
        effective_rate = rate_limit if rate_limit is not None else self.default_rate
        if effective_rate is None:
            return True  # unlimited

        # Calculate how long to wait between tokens
        wait_per_token = 1.0 / self._tokens_per_second(effective_rate)

        while True:
            # Try to consume a token
            if self.consume_tokens(pipeline_name, 1, effective_rate):
                return True

            # Check if we've exceeded max wait time
            elapsed = time.time() - start_time
            if elapsed >= max_wait:
                return False

            # Wait for approximately one token's worth of time before retrying
            # This ensures we're respecting the rate limit
            remaining_wait = max_wait - elapsed
            sleep_time = min(wait_per_token, remaining_wait)
            time.sleep(sleep_time)

