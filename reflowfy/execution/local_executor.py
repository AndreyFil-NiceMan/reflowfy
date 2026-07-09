"""Local executor for testing and debugging."""

import logging
import uuid
from typing import Any, Dict, Optional

from reflowfy.core.execution_context import (
    ExecutionContext,
    build_flat_runtime_params,
)
from reflowfy.execution.base import BaseExecutor, ExecutionState, ExecutionStatus
from reflowfy.execution.job_runner import plan_slices, run_job_records

logger = logging.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """
    Local executor for /test mode.

    Executes pipeline in-process:
    1. Fetches limited data from source
    2. Applies transformations sequentially
    3. Sends to destination directly
    4. Returns result immediately

    No Kafka. No workers. No async processing.
    """

    def __init__(self, max_records: int = 100):
        """
        Initialize local executor.

        Args:
            max_records: Maximum records to fetch for testing
        """
        self.max_records = max_records

    def execute(
        self,
        pipeline: Any,
        runtime_params: Dict[str, Any],
        execution_id: Optional[str] = None,
    ) -> ExecutionStatus:
        """
        Execute pipeline locally.

        Args:
            pipeline: Pipeline instance
            runtime_params: Runtime parameters
            execution_id: Optional execution ID

        Returns:
            ExecutionStatus with results
        """
        from reflowfy.core.id_based_pipeline import IdBasedPipeline

        if execution_id is None:
            execution_id = str(uuid.uuid4())

        # Check if this is an IdBasedPipeline — use per-ID execution
        if isinstance(pipeline, IdBasedPipeline):
            return self._execute_id_based(pipeline, runtime_params, execution_id)

        # Resolve pipeline with runtime params (for AbstractPipeline).
        # _resolved_params includes defaults + any keys added by define_source.
        if hasattr(pipeline, "resolve"):
            pipeline.resolve(runtime_params)

        # Create execution context
        resolved_params = dict(getattr(pipeline, "_resolved_params", runtime_params))

        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            runtime_params=resolved_params,
        )

        # Build one shared mutable runtime_params dict for the full
        # transformation chain and destination metadata.
        flat_runtime_params = build_flat_runtime_params(
            resolved_params,
            execution_id=context.execution_id,
            batch_id=context.batch_id or "",
            pipeline_name=context.pipeline_name,
            created_at=context.created_at.isoformat(),
            batch_number=context.batch_number,
            total_batches=context.total_batches,
            retry_count=context.retry_count,
            is_retry=context.is_retry,
        )

        # Initialize status
        status = ExecutionStatus(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            state=ExecutionState.RUNNING,
            total_jobs=1,
        )

        try:
            # Plan slices exactly as the manager does, then run each through the
            # same v2 core the worker uses (fetch → normalize → transform →
            # resolve destination). max_records caps the preview across slices.
            total_fetched = 0
            total_sent = 0
            for sub in plan_slices(pipeline.source, flat_runtime_params):
                remaining = self.max_records - total_fetched
                if remaining <= 0:
                    break
                logger.debug("Fetching data from source (limit: %d)", remaining)
                records, transformed_records, applied, destination = run_job_records(
                    sub, pipeline, flat_runtime_params, limit=remaining
                )

                if not records:
                    continue

                logger.debug("Fetched %d records", len(records))
                for name, _duration in applied:
                    logger.debug("Applied transformation: %s", name)

                logger.debug("Sending %d records to destination", len(transformed_records))
                destination.send_with_retry(
                    transformed_records,
                    metadata=flat_runtime_params,
                )
                total_fetched += len(records)
                total_sent += len(transformed_records)

            if total_fetched == 0:
                logger.info("Execution %s: no records fetched", execution_id)
                status.state = ExecutionState.COMPLETED
                status.completed_jobs = 1
                status.metadata["records_count"] = 0
                return status

            logger.info(
                "Execution %s completed: %d fetched, %d sent",
                execution_id,
                total_fetched,
                total_sent,
            )
            status.state = ExecutionState.COMPLETED
            status.completed_jobs = 1
            status.metadata["records_fetched"] = total_fetched
            status.metadata["records_sent"] = total_sent

            return status

        except Exception as e:
            logger.error("Execution %s failed: %s", execution_id, e, exc_info=True)

            status.state = ExecutionState.FAILED
            status.failed_jobs = 1
            status.error_message = str(e)

            return status

    def _execute_id_based(
        self,
        pipeline: Any,
        runtime_params: Dict[str, Any],
        execution_id: str,
    ) -> ExecutionStatus:
        """
        Execute an IdBasedPipeline locally.

        Iterates over each ID, resolves source per-ID, fetches limited data,
        applies transformations, and sends to shared destination.

        Args:
            pipeline: IdBasedPipeline instance
            runtime_params: Runtime parameters (must include 'ids')
            execution_id: Execution ID

        Returns:
            ExecutionStatus with results
        """
        # Resolve and validate
        pipeline.resolve(runtime_params)
        params = pipeline.apply_defaults(runtime_params)
        ids = params.get("ids", [])

        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            runtime_params=params,
        )

        status = ExecutionStatus(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            state=ExecutionState.RUNNING,
            total_jobs=len(ids),
        )

        logger.info("IdBasedPipeline %s local execution: %d IDs", execution_id, len(ids))

        total_records_fetched = 0
        total_records_sent = 0

        try:
            for current_id in ids:
                logger.debug("Processing ID: %s", current_id)

                # Resolve source for this ID.
                # batch_params is a per-ID copy enriched by define_source.
                resolved = pipeline.resolve_for_id(params, current_id)
                source = resolved["source"]
                batch_params = resolved.get("batch_params", params)

                # Build flat mutable runtime_params for this ID's chain.
                flat_id_params = build_flat_runtime_params(
                    batch_params,
                    execution_id=context.execution_id,
                    batch_id=context.batch_id or "",
                    pipeline_name=context.pipeline_name,
                    created_at=context.created_at.isoformat(),
                    batch_number=context.batch_number,
                    total_batches=context.total_batches,
                    retry_count=context.retry_count,
                    is_retry=context.is_retry,
                    current_ids=[current_id],
                )
                flat_id_params["current_id"] = current_id

                # Plan slices and run each through the shared v2 core, exactly as
                # the worker does (capped at max_records for the preview).
                id_fetched = 0
                id_sent = 0
                for sub in plan_slices(source, flat_id_params):
                    remaining = self.max_records - id_fetched
                    if remaining <= 0:
                        break
                    records, transformed_records, applied, destination = run_job_records(
                        sub, pipeline, flat_id_params, limit=remaining
                    )
                    if not records:
                        continue
                    for name, _duration in applied:
                        logger.debug("Applied: %s", name)
                    destination.send_with_retry(
                        transformed_records,
                        metadata=flat_id_params,
                    )
                    id_fetched += len(records)
                    id_sent += len(transformed_records)

                if id_fetched == 0:
                    logger.debug("No records for ID: %s", current_id)
                    status.completed_jobs += 1
                    continue

                total_records_fetched += id_fetched
                total_records_sent += id_sent
                status.completed_jobs += 1
                logger.debug("ID %s: fetched %d, sent %d", current_id, id_fetched, id_sent)

            status.state = ExecutionState.COMPLETED
            status.metadata["ids_processed"] = len(ids)
            status.metadata["records_fetched"] = total_records_fetched
            status.metadata["records_sent"] = total_records_sent

            logger.info(
                "IdBasedPipeline %s complete: %d IDs, %d records",
                execution_id,
                len(ids),
                total_records_sent,
            )
            return status

        except Exception as e:
            logger.error(
                "IdBasedPipeline %s execution failed: %s", execution_id, e, exc_info=True
            )
            status.state = ExecutionState.FAILED
            status.failed_jobs = len(ids) - status.completed_jobs
            status.error_message = str(e)
            return status
