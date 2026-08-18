"""
Example reusable transformation.

Transformations auto-register via metaclass when they subclass BaseTransformation.
Just define the class and import it in any pipeline.
"""

from typing import Any, Dict

from reflowfy import BaseTransformation, PipelineError, Records, get_logger

# There is no `self` here — get_logger works from any module, function or class.
# The worker attaches execution_id / job_id / pipeline_name to every record.
logger = get_logger(__name__)


class ExampleTransform(BaseTransformation):
    """Example transformation that adds processing metadata."""

    name = "example_transform"

    def apply(self, records: Records, runtime_params: Dict[str, Any]) -> Records:
        """
        Transform a batch of records.

        Args:
            records: List of record dicts
            runtime_params: Flat dict of user params + execution-context keys
                (execution_id, batch_id, pipeline_name, created_at, …).
                Mutations are visible to subsequent transformations and the destination.

        Returns:
            Transformed list of records
        """
        execution_id = runtime_params.get("execution_id", "unknown")

        # Raise to fail the job deliberately. The message, type and traceback are
        # stored on the job record, and the error names the step that failed.
        if runtime_params.get("strict") and not records:
            raise PipelineError("strict mode: expected at least one record")

        logger.info("Tagging %d records for execution %s", len(records), execution_id)
        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id
        return records
