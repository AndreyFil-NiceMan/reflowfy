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
    
    Stores job payload for dispatch and tracking data for progress.
    """
    
    __tablename__ = "jobs"
    
    job_id = Column(String(255), primary_key=True)
    execution_id = Column(String(255), ForeignKey("executions.execution_id"), nullable=False, index=True)
    
    # Job data
    job_payload = Column(JSON, nullable=False)
    batch_number = Column(Integer, nullable=True)
    
    # State tracking
    state = Column(String(50), nullable=False, index=True)  # pending, dispatched, completed, failed
    
    # Worker results
    processed_records = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    stats = Column(JSON, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    dispatched_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Relationship to execution
    execution = relationship("Execution", back_populates="jobs")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "job_id": self.job_id,
            "execution_id": self.execution_id,
            "state": self.state,
            "batch_number": self.batch_number,
            "processed_records": self.processed_records,
            "error_message": self.error_message,
            "stats": self.stats,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "dispatched_at": self.dispatched_at.isoformat() if self.dispatched_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


# Backward compatibility alias
Checkpoint = Job
