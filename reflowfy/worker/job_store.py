"""The worker's writes to its job row, and its content-dedup claims.

The claim (``pending|dispatched → running`` with a fresh token in ``claimed_by``) fences
every later write: the heartbeat, the outcome and a ``job_retry`` reschedule all match
``state = 'running' AND claimed_by = <token>`` (see CLAUDE.md "The claim fences the
worker's writes"). ``WorkerExecutor`` (``executor``) runs the job on top of this.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from reflowfy.core.job_retry import ERROR_HISTORY_MAX, JobRetryConfig, describe_error
from reflowfy.execution.content_dedup import (
    claim_record_hashes,
    compute_record_hash,
    destination_identity,
    record_identity,
    release_record_hashes,
)
from reflowfy.destinations.record import RecordDestination
from reflowfy.observability.logging import get_logger
from reflowfy.store.db_settings import async_connect_args
from reflowfy.store.db_settings import database_url as env_database_url
from reflowfy.store.models import Execution, Job
from reflowfy.store.leases import JobLeases
from reflowfy.store.states import JobState, ExecutionState

from reflowfy.worker.job_stats import JobStats

logger = get_logger("worker.executor")


class JobStore:
    """A worker's database session factory and every write it makes about a job."""

    #: Waits between attempts at saving a job's outcome. A brief Postgres outage
    #: (restart, failover) should not make a finished job run again.
    _save_retry_delays: Tuple[float, ...] = (1.0, 2.0, 4.0)

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize worker executor.

        Args:
            database_url: PostgreSQL connection URL
        """
        sync_url = database_url or env_database_url()

        # Convert sync URL to async (postgresql:// -> postgresql+asyncpg://)
        if sync_url.startswith("postgresql://"):
            self.database_url = sync_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        else:
            self.database_url = sync_url

        # Create async database engine and session factory
        self._engine = create_async_engine(
            self.database_url,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False,
            connect_args=async_connect_args(self.database_url),
        )
        self._async_session = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def close(self) -> None:
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()

    async def _claim_job(self, job_id: str, stats: Optional["JobStats"] = None) -> Optional[str]:
        """Claim a job for this delivery: pending/dispatched -> running, attempts + 1,
        and a fresh ``claimed_by`` token (kept on ``stats.claim_token``) that fences this
        delivery's later writes.

        Returns None when this delivery should run the job — it claimed it, or the
        job has no row (an untracked message). Otherwise returns the state that
        blocks it (running elsewhere, or already finished).

        Fails open: if the database is unreachable the job runs anyway, which is
        the at-least-once behaviour from before claims existed.
        """
        from sqlalchemy import select, update

        from sqlalchemy import or_

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        token = uuid.uuid4().hex
        try:
            async with self._async_session() as db:
                # A pending job whose job_retry backoff has not passed is not claimed: an
                # old message delivered again must not cut the backoff short.
                result = await db.execute(
                    update(Job)
                    .where(
                        Job.job_id == job_id,
                        Job.state.in_(JobState.UNSTARTED),
                        or_(Job.next_attempt_at.is_(None), Job.next_attempt_at <= now),
                    )
                    .values(
                        state=JobState.RUNNING,
                        attempts=Job.attempts + 1,
                        updated_at=now,
                        next_attempt_at=None,
                        claimed_by=token,
                    )
                )
                if (getattr(result, "rowcount", 0) or 0) == 1:
                    await db.commit()
                    if stats is not None:
                        stats.claim_token = token
                    return None
                state = (
                    await db.execute(select(Job.state).where(Job.job_id == job_id))
                ).scalar_one_or_none()
                await db.commit()
                return state
        except Exception:
            logger.warning(
                "Could not claim job (database error); running it anyway",
                exc_info=True,
                extra={"event": "job_claim_error"},
            )
            return None

    async def _heartbeat_job(self, job_id: str, claim_token: Optional[str] = None) -> None:
        """Refresh the running job's ``updated_at`` every ``JobLeases.heartbeat_seconds``.

        The manager requeues a ``running`` job whose ``updated_at`` is older than its
        running lease; this keeps a live job from looking dead. A failed beat is
        logged and retried — never raised into the job. Only this delivery's claim
        (``claim_token``) is refreshed: a stalled worker must not keep alive the lease of
        the delivery that took its job over.
        """
        from sqlalchemy import update

        interval = JobLeases.from_env().heartbeat_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                async with self._async_session() as db:
                    await db.execute(
                        update(Job)
                        .where(*self._owned(job_id, claim_token))
                        .values(updated_at=datetime.now(timezone.utc).replace(tzinfo=None))
                    )
                    await db.commit()
            except Exception:
                logger.warning(
                    "Heartbeat failed; will retry",
                    exc_info=True,
                    extra={"event": "heartbeat_error"},
                )

    @staticmethod
    def _owned(job_id: str, claim_token: Optional[str]) -> Tuple[Any, ...]:
        """WHERE clauses matching the job only while this delivery owns it."""
        clauses: Tuple[Any, ...] = (Job.job_id == job_id, Job.state == JobState.RUNNING)
        if claim_token is not None:
            clauses += (Job.claimed_by == claim_token,)
        return clauses

    async def _still_owns(self, job_id: str, stats: "JobStats") -> bool:
        """Whether this delivery still owns the job; marks ``stats.claim_lost`` if not.

        True without a claim token (no row, or the claim failed open) and on a database
        error: as for the claim itself, the job goes ahead rather than not at all.
        """
        if stats.claim_token is None:
            return True
        from sqlalchemy import select

        try:
            async with self._async_session() as db:
                owned = (
                    await db.execute(
                        select(Job.job_id).where(*self._owned(job_id, stats.claim_token))
                    )
                ).first()
                state = None
                if owned is None:
                    state = (
                        await db.execute(select(Job.state).where(Job.job_id == job_id))
                    ).scalar_one_or_none()
        except Exception:
            logger.warning(
                "Could not check the job's claim (database error); going ahead",
                exc_info=True,
                extra={"event": "job_claim_check_error"},
            )
            return True
        if owned is None:
            self._claim_lost(stats, state)
            return False
        return True

    @staticmethod
    def _claim_lost(stats: "JobStats", state: Optional[str]) -> None:
        stats.claim_lost = True
        logger.warning(
            "Job was taken from this worker (now %s) before it finished — its lease "
            "expired; the outcome is left to the delivery that owns it",
            state,
            extra={"event": "job_claim_lost", "state": state},
        )

    async def _update_job_in_db(
        self,
        execution_id: str,
        job_id: str,
        stats: JobStats,
        retry_in: Optional[float] = None,
    ) -> bool:
        """
        Update job status directly in PostgreSQL asynchronously.

        Note: Only updates the jobs table. The reflow manager syncs
        execution counts from the jobs table via _sync_counts_from_db().

        Args:
            execution_id: Execution ID
            job_id: Job ID
            stats: Job statistics
            retry_in: Schedule a job_retry retry this many seconds from now: the job goes
                back to ``pending`` with ``next_attempt_at`` set, ``attempts`` reset (each
                retry gets the full JOB_MAX_ATTEMPTS lease budget), and the error kept.
                Only while the execution has not ended; otherwise nothing is written.

        Returns:
            True if the row was written.
        """
        if stats.claim_lost:
            return False
        if stats.job_retry_enabled and stats.retry_count is None:
            # Not a failure (those set it): keep what earlier runs of this job recorded.
            previous = await self._previous_retries(job_id)
            if previous is not None and previous[0]:
                stats.retry_count, stats.error_history = previous
        for attempt, delay in enumerate((*self._save_retry_delays, None), start=1):
            try:
                return await self._write_job_row(execution_id, job_id, stats, retry_in)
            except Exception as e:
                if delay is None:
                    # Left `running`: once its heartbeat stops, the lease sweep puts it
                    # back to pending after JOB_RUNNING_LEASE_SECONDS and it runs again.
                    logger.error(
                        "Job outcome not saved after %d attempts: %s",
                        attempt,
                        e,
                        exc_info=True,
                        extra={"event": "job_outcome_not_saved", "attempts": attempt},
                    )
                    return False
                logger.warning(
                    "Failed to update database (attempt %d), retrying in %.0fs: %s",
                    attempt,
                    delay,
                    e,
                    extra={"event": "db_update_error", "attempt": attempt},
                )
                await asyncio.sleep(delay)
        return False  # unreachable: the last attempt returns or logs and returns

    async def _write_job_row(
        self,
        execution_id: str,
        job_id: str,
        stats: JobStats,
        retry_in: Optional[float],
    ) -> bool:
        """One attempt at the job row write. True if written, False if no row matched."""
        async with self._async_session() as db:
            try:
                from datetime import timedelta

                from sqlalchemy import select, update

                state = stats.stored_state
                now = datetime.now(timezone.utc).replace(tzinfo=None)

                # Update job state
                update_data: Dict[str, Any] = {
                    "state": state,
                    "updated_at": now,
                    "completed_at": now,
                    "processed_records": stats.records_output,
                    "stats": stats.to_dict(),
                }
                if retry_in is not None:
                    update_data.update(
                        completed_at=None,
                        attempts=0,
                        next_attempt_at=now + timedelta(seconds=retry_in),
                    )

                if stats.error:
                    update_data["error_message"] = stats.error
                if stats.error_traceback:
                    update_data["error_traceback"] = stats.error_traceback

                # Only while this delivery owns the job (see _claim_job): it used to be
                # by job_id alone, so a worker whose lease expired overwrote a job that
                # was cancelled, failed by the sweep, or running on another worker.
                stmt = update(Job).where(*self._owned(job_id, stats.claim_token))
                if retry_in is not None:
                    # A job of an execution that was cancelled or force-failed meanwhile
                    # is not put back in the queue (nothing would ever take it).
                    stmt = stmt.where(
                        Job.execution_id.in_(
                            select(Execution.execution_id).where(
                                Execution.execution_id == execution_id,
                                Execution.state.in_(ExecutionState.ACTIVE),
                            )
                        )
                    )
                result = await db.execute(stmt.values(**update_data))

                rowcount = getattr(result, "rowcount", None)
                if rowcount == 0:
                    await db.rollback()
                    current: Any = (
                        await db.execute(
                            select(Job.state, Job.claimed_by).where(Job.job_id == job_id)
                        )
                    ).first()
                    if current is None:
                        logger.warning(
                            "Job not found in database", extra={"event": "job_row_missing"}
                        )
                    elif current[0] != JobState.RUNNING or (
                        stats.claim_token is not None and current[1] != stats.claim_token
                    ):
                        self._claim_lost(stats, current[0])
                    # else: a retry refused because the execution ended (retry_in).
                    return False

                await db.commit()
                logger.debug("Status updated in database", extra={"event": "job_status_saved"})
                return True

            except Exception:
                await db.rollback()
                raise

    async def _previous_retries(self, job_id: str) -> Optional[Tuple[int, List[Dict[str, str]]]]:
        """The retry count and error history earlier runs of this job left in its stats.

        ``None`` when they cannot be read: the caller must not guess. Counting from
        zero made every failure look like the first, so ``max_retries`` was never
        reached while the read kept failing.
        """
        from sqlalchemy import select

        try:
            async with self._async_session() as db:
                previous: Any = (
                    await db.execute(select(Job.stats).where(Job.job_id == job_id))
                ).scalar_one_or_none()
        except Exception:
            logger.warning(
                "Could not read the job's earlier retries",
                exc_info=True,
                extra={"event": "job_retry_read_error"},
            )
            return None
        if not isinstance(previous, dict):
            return 0, []
        stats_dict = cast(Dict[str, Any], previous)
        count: Any = stats_dict.get("retry_count")
        history: Any = stats_dict.get("error_history")
        return (
            count if isinstance(count, int) and not isinstance(count, bool) else 0,
            (
                [cast(Dict[str, str], h) for h in cast(List[Any], history) if isinstance(h, dict)]
                if isinstance(history, list)
                else []
            ),
        )

    async def _finish_failed_job(
        self,
        job_payload: Dict[str, Any],
        execution_id: str,
        job_id: str,
        stats: JobStats,
        error: BaseException,
    ) -> None:
        """Record a failed job: ``failed``, or ``pending`` again when ``job_retry`` says so.

        Retried when the job message carries a ``job_retry`` config, the error matches it
        and retries are left: the job goes back to ``pending`` with ``next_attempt_at`` set
        after the backoff, and the drive loop sends it again then. The content-dedup
        claims were released before this, so the retry re-claims its records.
        """
        try:
            retry = JobRetryConfig.from_payload(job_payload)
        except Exception:
            logger.warning(
                "Ignoring an unreadable job_retry config",
                exc_info=True,
                extra={"event": "job_retry_config_error"},
            )
            retry = None
        if retry is None:
            await self._update_job_in_db(execution_id, job_id, stats)
            return

        previous = await self._previous_retries(job_id)
        if previous is None:
            logger.warning(
                "Job failed (%s), not retried: its earlier retries could not be read",
                type(error).__name__,
                extra={"event": "job_retry_exhausted", "error_type": type(error).__name__},
            )
            stats.error_history = [describe_error(error)]
            await self._update_job_in_db(execution_id, job_id, stats)
            return
        previous_count, history = previous
        stats.retry_count = previous_count
        stats.error_history = (history + [describe_error(error)])[-ERROR_HISTORY_MAX:]
        error_type = type(error).__name__
        if retry.should_retry(error) and previous_count < retry.max_retries:
            retry_number = previous_count + 1
            delay = retry.delay_for(retry_number, error)
            stats.retry_count = retry_number
            stats.retry_scheduled = True
            stats.retry_in_seconds = round(delay, 3)
            if await self._update_job_in_db(execution_id, job_id, stats, retry_in=delay):
                logger.info(
                    "Job failed (%s), retry %d/%d in %.0fs",
                    error_type,
                    retry_number,
                    retry.max_retries,
                    delay,
                    extra={
                        "event": "job_retry_scheduled",
                        "error_type": error_type,
                        "retry": retry_number,
                        "max_retries": retry.max_retries,
                        "retry_in_s": round(delay, 3),
                    },
                )
                return
            # The execution ended meanwhile (cancelled, force-failed): no retry.
            stats.retry_count = previous_count
            stats.retry_scheduled = False
            stats.retry_in_seconds = None
            if stats.claim_lost:
                return  # not ours to write any more (logged by _write_job_row)
        else:
            reason = (
                f"{previous_count} retr{'y' if previous_count == 1 else 'ies'} used"
                if retry.should_retry(error)
                else "error is not retried"
            )
            logger.info(
                "Job failed (%s), not retried: %s",
                error_type,
                reason,
                extra={
                    "event": "job_retry_exhausted",
                    "error_type": error_type,
                    "retry_count": previous_count,
                },
            )
        await self._update_job_in_db(execution_id, job_id, stats)

    async def _claim_records(
        self,
        pipeline: Any,
        pipeline_name: str,
        job_id: str,
        transformed_records: List[Any],
        job_params: Optional[Dict[str, Any]],
        destination: Any,
        runtime_params: Any,
    ) -> Tuple[List[Any], List[bytes]]:
        """Claim a hash per (record, destination) about to be sent.

        Returns (records to send, claimed). A pair whose hash another job holds was
        already written there and is dropped, as is a second record with the same
        identity within this job. A record destination (``@destination(per_record=True)``,
        ``fan_out``) writes each record somewhere of its own, maybe several places:
        each is hashed, so the same record sent to two topics is two writes, and a
        record already in one is still sent to the other (``keep_only``). A record
        left with nowhere to go is dropped.
        """
        dedup_key = getattr(pipeline, "dedup_key", None)
        identities: Dict[int, Dict[str, Any]] = {}

        def identity_of(target: Any) -> Dict[str, Any]:
            if id(target) not in identities:
                identities[id(target)] = destination_identity(target)
            return identities[id(target)]

        def targets_of(record: Any) -> List[Any]:
            if isinstance(destination, RecordDestination):
                return list(destination.destinations_for(record))
            return [destination]

        hashed: List[Tuple[Any, List[Tuple[Any, bytes]]]] = []
        for record in transformed_records:
            identity = record_identity(record, dedup_key, runtime_params)
            hashed.append(
                (
                    record,
                    [
                        (
                            target,
                            compute_record_hash(
                                pipeline_name, identity, identity_of(target), job_params
                            ),
                        )
                        for target in targets_of(record)
                    ],
                )
            )
        won = await claim_record_hashes(
            self._async_session, [h for _, pairs in hashed for _, h in pairs], job_id
        )
        kept: List[Any] = []
        seen: Set[bytes] = set()
        for record, pairs in hashed:
            new_targets = [target for target, h in pairs if h in won and h not in seen]
            seen.update(h for _, h in pairs)
            if not new_targets:
                continue
            if isinstance(destination, RecordDestination) and len(new_targets) < len(pairs):
                destination.keep_only(record, new_targets)
            kept.append(record)
        return kept, list(won)

    async def _release_claims(self, record_hashes: Any, job_id: str) -> None:
        """Give back this job's content-dedup claims; a failure is logged, not raised.

        Left held, the claims make a re-run of this job drop its records as already
        written until CONTENT_DEDUP_RETENTION_HOURS purges them.
        """
        try:
            await release_record_hashes(self._async_session, record_hashes, job_id)
        except Exception:
            logger.warning(
                "Could not release %d content-dedup claims",
                len(record_hashes),
                exc_info=True,
                extra={"event": "dedup_release_failed"},
            )
