"""Database models for ReflowManager."""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, String, Integer, Float, Text, DateTime, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Execution(Base):
    """
    Pipeline execution record.
    
    Tracks the state and progress of a pipeline execution.
    """
    
    __tablename__ = "executions"
    
    execution_id = Column(String(255), primary_key=True)
    pipeline_name = Column(String(255), nullable=False, index=True)
    state = Column(String(50), nullable=False, index=True)  # pending, running, paused, completed, failed
    total_jobs = Column(Integer, default=0)
    jobs_dispatched = Column(Integer, default=0)
    jobs_completed = Column(Integer, default=0)
    jobs_failed = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    runtime_params = Column(JSON, nullable=True)
    
    # Relationship to checkpoints
    checkpoints = relationship("Checkpoint", back_populates="execution", cascade="all, delete-orphan")
    
    # Relationship to jobs
    jobs = relationship("Job", back_populates="execution", cascade="all, delete-orphan")
    
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
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "runtime_params": self.runtime_params,
        }


class Checkpoint(Base):
    """
    Checkpoint for resumable pipeline execution.
    
    Stores the state of individual batches/jobs to enable pause/resume.
    """
    
    __tablename__ = "checkpoints"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(255), ForeignKey("executions.execution_id"), nullable=False, index=True)
    batch_id = Column(String(255), nullable=False, index=True)
    offset_data = Column(JSON, nullable=True)  # Source-specific offset/cursor data
    processed_records = Column(Integer, default=0)
    state = Column(String(50), nullable=False)  # pending, processing, completed, failed
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    error_message = Column(Text, nullable=True)
    stats = Column(JSON, nullable=True)  # Detailed job statistics from worker
    
    # Relationship to execution
    execution = relationship("Execution", back_populates="checkpoints")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "offset_data": self.offset_data,
            "processed_records": self.processed_records,
            "state": self.state,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "error_message": self.error_message,
            "stats": self.stats,
        }


class RateLimitState(Base):
    """
    Rate limiter token bucket state.
    
    Stores the current token count for each pipeline's rate limiter.
    """
    
    __tablename__ = "rate_limit_state"
    
    pipeline_name = Column(String(255), primary_key=True)
    tokens = Column(Float, nullable=False)
    max_tokens = Column(Float, nullable=False)
    refill_rate = Column(Float, nullable=False)  # tokens per second
    last_update = Column(DateTime, nullable=False)
    
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
    
    Stores the full job payload in PostgreSQL instead of RAM,
    enabling crash recovery and large dataset processing.
    """
    
    __tablename__ = "jobs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(255), ForeignKey("executions.execution_id"), nullable=False, index=True)
    batch_id = Column(String(255), nullable=False, unique=True, index=True)
    job_payload = Column(JSON, nullable=False)  # Full job data
    state = Column(String(50), nullable=False, index=True)  # pending, dispatched, completed, failed
    batch_number = Column(Integer, nullable=True)  # Checkpoint batch number
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    dispatched_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Relationship to execution
    execution = relationship("Execution", back_populates="jobs")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "execution_id": self.execution_id,
            "batch_id": self.batch_id,
            "state": self.state,
            "batch_number": self.batch_number,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "dispatched_at": self.dispatched_at.isoformat() if self.dispatched_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }
