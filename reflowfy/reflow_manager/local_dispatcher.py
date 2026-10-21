"""Local job dispatcher for ReflowManager."""

import logging
from typing import Dict, Any, List, Optional


from reflowfy.reflow_manager.dispatcher import (
    TOKEN_WAIT_SECONDS,
    BaseDispatcher,
    OnSent,
    ShouldStop,
)
from reflowfy.reflow_manager.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class LocalDispatcher(BaseDispatcher):
    """
    Dispatches jobs locally by running them in-process.

    Creates a fresh WorkerExecutor for each batch to avoid asyncpg
    connection issues across event loop boundaries (each batch runs
    in its own asyncio.run() via _run_async).

    Supports rate limiting via the same token-bucket RateLimiter
    used by KafkaDispatcher.
    """

    def __init__(self, rate_limiter: RateLimiter, db_session: Any = None):
        super().__init__(rate_limiter)

    def _create_executor(self):
        """Create a fresh WorkerExecutor instance."""
        from reflowfy.worker.executor import WorkerExecutor

        return WorkerExecutor()

    async def dispatch_job(
        self,
        job_payload: Dict[str, Any],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
    ) -> bool:
        """Dispatch single job locally with rate limiting."""
        # Rate limit: acquire a token before executing
        if rate_limit is not None:
            acquired = self.rate_limiter.acquire_token(pipeline_name, rate_limit, max_wait=300.0)
            if not acquired:
                logger.warning(
                    "Rate limit timeout, skipping job",
                    extra={"event": "rate_limit_timeout", "job_id": job_payload.get("job_id")},
                )
                return False

        executor = self._create_executor()
        try:
            await executor.execute_job(job_payload)
            return True
        except Exception:
            logger.error(
                "Local dispatch failed",
                exc_info=True,
                extra={"event": "local_dispatch_error", "job_id": job_payload.get("job_id")},
            )
            return False
        finally:
            await executor.close()

    async def dispatch_jobs_batch(
        self,
        jobs: List[Dict[str, Any]],
        pipeline_name: str,
        rate_limit: Optional[float] = None,
        on_sent: Optional[OnSent] = None,
        should_stop: Optional[ShouldStop] = None,
        on_unsendable: Optional[Any] = None,  # nothing is serialized locally: never unsendable
    ) -> int:
        """Dispatch batch locally with rate limiting and a fresh executor."""
        executor = self._create_executor()
        dispatched = 0

        try:
            for job in jobs:
                if should_stop and should_stop():
                    break
                # Rate limit: acquire a token before each job (same as KafkaDispatcher)
                if rate_limit is not None:
                    acquired = self.rate_limiter.acquire_token(
                        pipeline_name, rate_limit, max_wait=TOKEN_WAIT_SECONDS
                    )
                    if not acquired:
                        logger.debug(
                            "No rate-limit token yet after %d jobs; returning",
                            dispatched,
                            extra={"event": "rate_limit_wait", "jobs_dispatched": dispatched},
                        )
                        break

                try:
                    await executor.execute_job(job)
                except Exception:
                    logger.error(
                        "Local job execution failed",
                        exc_info=True,
                        extra={"event": "local_dispatch_error", "job_id": job.get("job_id")},
                    )
                # Counted whatever the outcome: the executor already recorded it on the
                # job row, and the caller needs "first N handled" to stay true.
                dispatched += 1
                if on_sent:
                    on_sent(job)
        finally:
            await executor.close()

        return dispatched

    async def close(self) -> None:
        """No persistent resources to close."""
        pass
