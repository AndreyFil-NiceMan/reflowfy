"""Local executor for testing and debugging."""

import uuid
from typing import Any, Dict, Optional

from reflowfy.core.execution_context import (
    ExecutionContext,
    build_flat_runtime_params,
)
from reflowfy.execution.base import BaseExecutor, ExecutionState, ExecutionStatus
from reflowfy.execution.transformation_runner import apply_transformations_iteratively


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
            # 1. Fetch data from source (limited)
            print(f"🔍 Fetching data from source (limit: {self.max_records})...")
            records = pipeline.source.fetch(resolved_params, limit=self.max_records)

            if not records:
                print("⚠️  No records fetched")
                status.state = ExecutionState.COMPLETED
                status.completed_jobs = 1
                status.metadata["records_count"] = 0
                return status

            print(f"✓ Fetched {len(records)} records")

            # 2 + 3. Resolve and apply transformations iteratively so that params
            # mutated mid-chain can reveal later transformations.
            transformed_records, applied = apply_transformations_iteratively(
                pipeline, records, flat_runtime_params
            )
            for name, _duration in applied:
                print(f"✓ Applied transformation: {name}")

            # 4. Resolve destination from post-transformation records and send
            destination = pipeline.define_destination(transformed_records, flat_runtime_params)
            print(f"📤 Sending {len(transformed_records)} records to destination...")

            destination.send_with_retry(
                transformed_records,
                metadata=flat_runtime_params,
            )

            print("✓ Records sent successfully")

            # 4. Update status
            status.state = ExecutionState.COMPLETED
            status.completed_jobs = 1
            status.metadata["records_fetched"] = len(records)
            status.metadata["records_sent"] = len(transformed_records)

            return status

        except Exception as e:
            print(f"❌ Execution failed: {e}")

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

        print(f"🔍 IdBasedPipeline local execution: {len(ids)} IDs")

        total_records_fetched = 0
        total_records_sent = 0

        try:
            for current_id in ids:
                print(f"\n  Processing ID: {current_id}")

                # Resolve source for this ID.
                # batch_params is a per-ID copy enriched by define_source.
                resolved = pipeline.resolve_for_id(params, current_id)
                source = resolved["source"]
                batch_params = resolved.get("batch_params", params)

                # 1. Fetch data
                records = source.fetch(batch_params, limit=self.max_records)
                if not records:
                    print(f"  ⚠️ No records for ID: {current_id}")
                    status.completed_jobs += 1
                    continue

                print(f"  ✓ Fetched {len(records)} records for {current_id}")
                total_records_fetched += len(records)

                # 2. Build flat mutable runtime_params for this ID's chain.
                transformed_records = records
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

                # Resolve and apply transformations iteratively for this ID's chain.
                transformed_records, applied = apply_transformations_iteratively(
                    pipeline, records, flat_id_params
                )
                for name, _duration in applied:
                    print(f"  ✓ Applied: {name}")

                # 3. Resolve destination from post-transformation records and send.
                destination = pipeline.define_destination(
                    transformed_records,
                    flat_id_params,
                )
                destination.send_with_retry(
                    transformed_records,
                    metadata=flat_id_params,
                )

                total_records_sent += len(transformed_records)
                status.completed_jobs += 1
                print(f"  ✓ Sent {len(transformed_records)} records for {current_id}")

            status.state = ExecutionState.COMPLETED
            status.metadata["ids_processed"] = len(ids)
            status.metadata["records_fetched"] = total_records_fetched
            status.metadata["records_sent"] = total_records_sent

            print(f"\n✓ IdBasedPipeline complete: {len(ids)} IDs, {total_records_sent} records")
            return status

        except Exception as e:
            print(f"❌ IdBasedPipeline execution failed: {e}")
            status.state = ExecutionState.FAILED
            status.failed_jobs = len(ids) - status.completed_jobs
            status.error_message = str(e)
            return status
