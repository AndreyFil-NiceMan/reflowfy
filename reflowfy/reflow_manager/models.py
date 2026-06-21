"""Database models for ReflowManager."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import String, Integer, Float, Text, DateTime, ForeignKey, JSON
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship

Base = declarative_base()


class Execution(Base):
    """
    Pipeline execution record.

    Tracks the state and progress of a pipeline execution.
    """

    __tablename__ = "executions"

    execution_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    total_jobs: Mapped[int] = mapped_column(Integer, default=0)
    jobs_dispatched: Mapped[int] = mapped_column(Integer, default=0)
    jobs_completed: Mapped[int] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[int] = mapped_column(Integer, default=0)
    deduplicated_jobs: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    runtime_params: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Relationship to jobs
    jobs: Mapped[List["Job"]] = relationship("Job", back_populates="execution", cascade="all, delete-orphan")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "execution_id": self.execution_id,
            "pipeline_name": self.pipeline_name,
            "state": self.state,
            "total_jobs": self.total_jobs,
            "jobs_dispatched": self.jobs_dispatched,
            "jobs_completed": self.jobs_completed,
            "jobs_failed": self.jobs_failed,
            "deduplicated_jobs": self.deduplicated_jobs or 0,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "runtime_params": self.runtime_params,
        }


class RateLimitState(Base):
    """
    Rate limiter token bucket state.

    Stores the current token count for each pipeline's rate limiter.
    """

    __tablename__ = "rate_limit_state"

    pipeline_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    tokens: Mapped[float] = mapped_column(Float, nullable=False)
    max_tokens: Mapped[float] = mapped_column(Float, nullable=False)
    refill_rate: Mapped[float] = mapped_column(Float, nullable=False)
    last_update: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "pipeline_name": self.pipeline_name,
            "tokens": self.tokens,
            "max_tokens": self.max_tokens,
            "refill_rate": self.refill_rate,
            "last_update": self.last_update.isoformat() if self.last_update else None,
        }


class Job(Base):
    """
    Job record for pipeline execution.

    Stores job payload for dispatch and tracking data for progress.
    """

    __tablename__ = "jobs"

    job_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    execution_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("executions.execution_id"), nullable=False, index=True
    )

    # Job data
    job_payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    batch_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # State tracking
    state: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Worker results
    processed_records: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stats: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    dispatched_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationship to execution
    execution: Mapped["Execution"] = relationship("Execution", back_populates="jobs")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "job_id": self.job_id,
            "execution_id": self.execution_id,
            "job_payload": self.job_payload,
            "state": self.state,
            "batch_number": self.batch_number,
            "processed_records": self.processed_records,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "stats": self.stats,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "dispatched_at": self.dispatched_at.isoformat() if self.dispatched_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


# Backward compatibility alias
Checkpoint = Job


class DLQJob(Base):
    """
    Dead Letter Queue job for scheduled reflow.

    Stores jobs from external services to be processed at a scheduled time.
    """

    __tablename__ = "dlq_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    delay_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending", index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=5)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    execution_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "job_payload": self.job_payload,
            "pipeline_name": self.pipeline_name,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "delay_minutes": self.delay_minutes,
            "status": self.status,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "execution_id": self.execution_id,
        }


class DLQJobArchive(Base):
    """
    Archived permanently failed DLQ jobs.

    Jobs are moved here after exceeding max_retries for historical tracking.
    """

    __tablename__ = "dlq_jobs_archive"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    job_payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    pipeline_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    delay_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    archived_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "job_payload": self.job_payload,
            "pipeline_name": self.pipeline_name,
            "delay_minutes": self.delay_minutes,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "archived_at": self.archived_at.isoformat() if self.archived_at else None,
        }


class PipelineSchedule(Base):
    """
    Persistent cron schedule state for a scheduled pipeline.

    One row per pipeline. The scheduler reads next_run_at on every poll
    and fires an execution when it has elapsed.

    Manual triggers (POST /run) update last_triggered_at and recalculate
    next_run_at to prevent overlapping executions.
    """

    __tablename__ = "pipeline_schedules"

    pipeline_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    cron_expression: Mapped[str] = mapped_column(String(255), nullable=False)
    next_run_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_triggered_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_execution_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    # Stored as string to avoid SQLAlchemy Boolean portability quirks
    enabled: Mapped[str] = mapped_column(String(10), default="true", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "pipeline_name": self.pipeline_name,
            "cron_expression": self.cron_expression,
            "next_run_at": self.next_run_at.isoformat() if self.next_run_at else None,
            "last_triggered_at": self.last_triggered_at.isoformat() if self.last_triggered_at else None,
            "last_execution_id": self.last_execution_id,
            "enabled": self.enabled == "true",
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
