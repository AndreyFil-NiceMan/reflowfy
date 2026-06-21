"""Worker job executor with async support."""

import importlib
import os
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

from reflowfy.core.execution_context import build_flat_runtime_params_from_metadata
from reflowfy.core.registry import pipeline_registry
from reflowfy.destinations.api import ApiDestination
from reflowfy.destinations.console import ConsoleDestination
from reflowfy.destinations.kafka import KafkaDestination
from reflowfy.execution.transformation_runner import apply_transformations_iteratively
from reflowfy.reflow_manager.models import Job
from reflowfy.transformations.registry import transformation_registry


class JobStats:
    """Statistics for a job execution."""

    def __init__(self):
        """Initialize job statistics."""
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.records_input = 0
        self.records_output = 0
        self.transformation_times = {}
        self.destination_write_time: float = 0.0
        self.error: Optional[str] = None
        self.error_traceback: Optional[str] = None
        self.success = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        duration = self.end_time - self.start_time if self.end_time else 0

        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 3),
            "records_input": self.records_input,
            "records_output": self.records_output,
            "throughput_records_per_second": round(self.records_output / duration, 2)
            if duration > 0
            else 0,
            "transformation_times": self.transformation_times,
            "destination_write_time": round(self.destination_write_time, 3),
            "error": self.error,
            "success": self.success,
        }


class WorkerExecutor:
    """
    Executes jobs on worker nodes with async I/O.

    Responsibilities:
    1. Load transformations from registry
    2. Apply transformations to records
    3. Check destination health (async)
    4. Send to destination with retries (async)
    5. Rate limiting
    6. Update job status directly in PostgreSQL (async)
    """

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize worker executor.

        Args:
            database_url: PostgreSQL connection URL
        """
        sync_url = database_url or os.getenv(
            "DATABASE_URL", "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy"
        )

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
        )
        self._async_session = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def execute_job(self, job_payload: Dict[str, Any]) -> bool:
        """
        Execute a single job asynchronously.

        Args:
            job_payload: Job payload from Kafka

        Returns:
            True if successful, False otherwise
        """
        # Initialize statistics
        stats = JobStats()

        execution_id = job_payload.get("execution_id", "unknown")
        job_id = job_payload.get("job_id", "unknown")
        _pipeline_name = job_payload.get("pipeline_name", "unknown")

        try:
            # Extract job data
            transformation_names = job_payload.get("transformations", [])
            transformation_specs = job_payload.get("transformation_specs", [])
            destination_config = job_payload.get("destination", {})
            records = job_payload.get("records", [])
            metadata = job_payload.get("metadata", {})

            # Build one shared flat mutable runtime_params dict for the job.
            # The same object is passed through all transformations and then
            # into destination.send_with_retry, so in-place enrichments flow
            # end-to-end within this job execution.
            runtime_params = build_flat_runtime_params_from_metadata(metadata)

            # Track input records
            stats.records_input = len(records)

            if not records:
                print(f"⚠️  Job {job_id}: No records to process")
                stats.success = True
                stats.records_output = 0
                stats.end_time = time.time()
                await self._update_job_in_db(execution_id, job_id, stats)
                return True

            print(f"🔄 Processing job {job_id}: {len(records)} records")

            # Load and apply transformations (CPU-bound, stays sync).
            pipeline_name = job_payload.get("pipeline_name")
            pipeline = pipeline_registry.get(pipeline_name) if pipeline_name else None

            if pipeline is not None:
                # Dynamic resolution: re-resolve after each step so params mutated
                # mid-chain can reveal later transformations (matches local/test).
                transformed_records, applied = apply_transformations_iteratively(
                    pipeline, records, runtime_params
                )
                for name, duration in applied:
                    stats.transformation_times[name] = round(duration, 3)
                    print(f"  ✓ {name}")
            else:
                # Fallback: pipeline not discoverable in this worker process — replay
                # the frozen transformation list from the producer (no dynamic tail).
                transformed_records = self._apply_frozen_transformations(
                    records, runtime_params, transformation_names, transformation_specs, stats
                )

            # Track output records
            stats.records_output = len(transformed_records)

            # Build the destination from the pipeline using THIS worker's
            # transformed records and full runtime_params (which carry the
            # execution context: execution_id, batch_id, ...). This mirrors
            # LocalExecutor, so a user-authored ``body`` in define_destination
            # reflects the real records/context rather than the manager's
            # context-less preview. Fall back to the serialized config only when
            # the pipeline is not discoverable in this worker process.
            if pipeline is not None:
                destination = pipeline.define_destination(transformed_records, runtime_params)
            else:
                destination = self._create_destination(destination_config)

            # Health check (async)
            if not await destination.health_check():
                print("❌ Destination health check failed")
                stats.success = False
                stats.error = "Destination health check failed"
                stats.end_time = time.time()
                await self._update_job_in_db(execution_id, job_id, stats)
                return False

            # Send to destination and track time (async)
            print(f"  📤 Sending {len(transformed_records)} records to destination...")
            dest_start = time.time()
            await destination.send_with_retry(transformed_records, runtime_params)
            stats.destination_write_time = time.time() - dest_start

            # Mark as successful
            stats.success = True
            stats.end_time = time.time()

            print(
                f"✓ Job {job_id} completed successfully (duration: {stats.end_time - stats.start_time:.2f}s)\n"
            )

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return True

        except Exception as e:
            print(f"❌ Job {job_id} failed: {e}")
            tb_str = traceback.format_exc()
            print(tb_str)

            # Mark as failed — capture both summary and full traceback
            stats.success = False
            stats.error = str(e)
            stats.error_traceback = tb_str
            stats.end_time = time.time()

            # Update job status in PostgreSQL (async)
            await self._update_job_in_db(execution_id, job_id, stats)

            return False

    def _apply_frozen_transformations(
        self,
        records: List[Any],
        runtime_params: Dict[str, Any],
        transformation_names: List[str],
        transformation_specs: List[Dict[str, Any]],
        stats: JobStats,
    ) -> List[Any]:
        """Replay a frozen transformation list (fallback when the pipeline is not
        discoverable in this worker process). No dynamic re-resolution."""
        transformed_records = records
        for idx, transformation_name in enumerate(transformation_names):
            print(f"  🔄 Applying: {transformation_name}")
            transform_start = time.time()

            # Prefer explicit transformation spec from producer process to
            # avoid name-collision ambiguity in registry for duplicated names.
            transformation = None
            if idx < len(transformation_specs):
                spec = transformation_specs[idx] or {}
                spec_name = spec.get("name")
                module_name = spec.get("module")
                class_name = spec.get("class_name")
                if spec_name == transformation_name and module_name and class_name:
                    module = importlib.import_module(module_name)
                    cls = getattr(module, class_name)
                    transformation = cls()

            if transformation is None:
                transformation = transformation_registry.create_instance(transformation_name)

            transformed_records = transformation.apply(transformed_records, runtime_params)

            transform_duration = time.time() - transform_start
            stats.transformation_times[transformation_name] = round(transform_duration, 3)
            print(
                f"  ✓ {transformation_name}: {len(transformed_records)} records "
                f"({transform_duration:.2f}s)"
            )
        return transformed_records

    async def _update_job_in_db(self, execution_id: str, job_id: str, stats: JobStats):
        """
        Update job status directly in PostgreSQL asynchronously.

        Note: Only updates the jobs table. The reflow manager syncs
        execution counts from the jobs table via _sync_counts_from_db().

        Args:
            execution_id: Execution ID
            job_id: Job ID
            stats: Job statistics
        """
        async with self._async_session() as db:
            try:
                from sqlalchemy import update

                state = "completed" if stats.success else "failed"
                now = datetime.now(timezone.utc).replace(tzinfo=None)

                # Update job state
                update_data = {
                    "state": state,
                    "updated_at": now,
                    "completed_at": now,
                    "processed_records": stats.records_output,
                    "stats": stats.to_dict(),
                }

                if stats.error:
                    update_data["error_message"] = stats.error
                if stats.error_traceback:
                    update_data["error_traceback"] = stats.error_traceback

                # Update the job using async execute
                stmt = update(Job).where(Job.job_id == job_id).values(**update_data)
                result = await db.execute(stmt)

                rowcount = getattr(result, "rowcount", None)
                if rowcount == 0:
                    print(f"  ⚠️  Job {job_id} not found in database")
                    await db.rollback()
                    return

                await db.commit()
                print("  ✓ Updated job status in database")

            except Exception as e:
                print(f"  ⚠️  Failed to update database: {e}")
                await db.rollback()

    def _create_destination(self, destination_config: Dict[str, Any]) -> Any:
        """
        Create destination instance from config.

        Args:
            destination_config: Destination configuration

        Returns:
            Destination instance
        """
        dest_type = destination_config.get("type", "")
        config = destination_config.get("config", {})

        if dest_type == "KafkaDestination":
            return KafkaDestination(**config)
        elif dest_type == "ApiDestination":
            return ApiDestination(**config)
        elif dest_type == "ConsoleDestination":
            return ConsoleDestination(**config)
        else:
            raise ValueError(f"Unknown destination type: {dest_type}")

    async def close(self):
        """Close database connections."""
        if self._engine:
            await self._engine.dispose()
