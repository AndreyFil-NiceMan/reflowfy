"""Local executor for testing and debugging."""

from typing import Any, Dict, Optional
from reflowfy.core.execution_context import ExecutionContext
from reflowfy.execution.base import BaseExecutor, ExecutionStatus, ExecutionState
from reflowfy.transformations.base import TransformationError
import uuid


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

        # Resolve pipeline with runtime params (for AbstractPipeline)
        if hasattr(pipeline, 'resolve'):
            pipeline.resolve(runtime_params)

        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            pipeline_name=pipeline.name,
            runtime_params=runtime_params,
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
            records = pipeline.source.fetch(runtime_params, limit=self.max_records)

            if not records:
                print("⚠️  No records fetched")
                status.state = ExecutionState.COMPLETED
                status.completed_jobs = 1
                status.metadata["records_count"] = 0
                return status

            print(f"✓ Fetched {len(records)} records")

            # 2. Apply transformations
            transformed_records = records

            for transformation in pipeline.transformations:
                print(f"🔄 Applying transformation: {transformation.name}")

                try:
                    transformation.validate_input(transformed_records)
                    transformed_records = transformation.apply(
                        transformed_records,
                        context.to_dict(),
                    )
                    transformation.validate_output(transformed_records)

                    print(f"✓ Transformation complete: {len(transformed_records)} records")

                except Exception as e:
                    raise TransformationError(
                        transformation_name=transformation.name,
                        message=str(e),
                        original_error=e,
                    )

            # 3. Send to destination
            print(f"📤 Sending {len(transformed_records)} records to destination...")

            pipeline.destination.send_with_retry(
                transformed_records,
                metadata=context.to_dict(),
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

        # Resolve destination once (shared)
        destination = pipeline.define_destination(params)

        total_records_fetched = 0
        total_records_sent = 0

        try:
            for current_id in ids:
                print(f"\n  Processing ID: {current_id}")

                # Resolve source and transformations for this ID
                resolved = pipeline.resolve_for_id(params, current_id)
                source = resolved["source"]
                transformations = resolved["transformations"]

                # 1. Fetch data
                records = source.fetch(params, limit=self.max_records)
                if not records:
                    print(f"  ⚠️ No records for ID: {current_id}")
                    status.completed_jobs += 1
                    continue

                print(f"  ✓ Fetched {len(records)} records for {current_id}")
                total_records_fetched += len(records)

                # 2. Apply transformations
                transformed_records = records
                id_context = {**context.to_dict(), "current_id": current_id}

                for transformation in transformations:
                    print(f"  🔄 Applying: {transformation.name}")
                    transformation.validate_input(transformed_records)
                    transformed_records = transformation.apply(
                        transformed_records,
                        id_context,
                    )
                    transformation.validate_output(transformed_records)

                # 3. Send to destination
                destination.send_with_retry(
                    transformed_records,
                    metadata=id_context,
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
