"""Rate limiter using token bucket algorithm for ReflowManager."""

import time
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from reflowfy.reflow_manager.models import RateLimitState


class RateLimiter:
    """
    Token bucket rate limiter with database-backed state.

    Provides per-pipeline rate limiting with persistent state
    that survives restarts and works across multiple instances.
    """

    def __init__(self, db_session: Session, default_rate: float = 100.0):
        """
        Initialize rate limiter.

        Args:
            db_session: SQLAlchemy database session
            default_rate: Default rate limit (tokens per second)
        """
        self.db = db_session
        self.default_rate = default_rate

    def _get_or_create_state(self, pipeline_name: str, rate_limit: float) -> RateLimitState:
        """Get or create rate limit state for a pipeline, updating rate if different."""
        state = self.db.query(RateLimitState).filter(
            RateLimitState.pipeline_name == pipeline_name
        ).first()

        if not state:
            # Start with 1 token for strict rate limiting from the beginning
            # (not full bucket which would allow initial burst)
            state = RateLimitState(
                pipeline_name=pipeline_name,
                tokens=1.0,  # Start with minimal tokens - no burst allowed
                max_tokens=max(1.0, rate_limit),
                refill_rate=rate_limit,
                last_update=datetime.utcnow(),
            )
            self.db.add(state)
            self.db.commit()
            self.db.refresh(state)
        elif state.refill_rate != rate_limit:
            # Update the rate limit if it's different from what's stored
            # This handles rate_limit overrides from API calls
            state.refill_rate = rate_limit
            state.max_tokens = max(1.0, rate_limit)
            self.db.commit()

        return state

    def _refill_tokens(self, state: RateLimitState) -> None:
        """Refill tokens based on elapsed time."""
        now = datetime.utcnow()
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
            rate_limit: Optional override rate limit

        Returns:
            True if dispatch is allowed, False otherwise
        """
        effective_rate = rate_limit if rate_limit is not None else self.default_rate
        state = self._get_or_create_state(pipeline_name, effective_rate)

        # Refill tokens
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
            rate_limit: The rate limit to use

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
            rate_limit: Optional override rate limit

        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        effective_rate = rate_limit if rate_limit is not None else self.default_rate

        # Get state with row-level lock to prevent concurrent consumption
        state = self._get_state_with_lock(pipeline_name, effective_rate)

        # Calculate how many tokens should have been added
        now = datetime.utcnow()
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
        max_wait: float = 60.0,
    ) -> bool:
        """
        Atomically acquire a token, waiting if necessary.

        This method will wait until a token becomes available or max_wait expires.
        With proper rate limiting, it will block for 1/rate_limit seconds between tokens.

        Args:
            pipeline_name: Pipeline name
            rate_limit: Optional override rate limit
            max_wait: Maximum time to wait for a token (seconds). Default 60s.

        Returns:
            True if token was acquired, False if timed out
        """
        start_time = time.time()
        effective_rate = rate_limit if rate_limit is not None else self.default_rate

        # Calculate how long to wait between tokens
        wait_per_token = 1.0 / effective_rate

        while True:
            # Try to consume a token
            if self.consume_tokens(pipeline_name, 1, rate_limit):
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

