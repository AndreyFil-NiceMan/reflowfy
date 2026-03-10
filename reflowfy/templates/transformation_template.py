"""
Example reusable transformation.

Transformations auto-register via metaclass when they subclass BaseTransformation.
Just define the class and import it in any pipeline.
"""

from reflowfy import BaseTransformation


class ExampleTransform(BaseTransformation):
    """Example transformation that adds processing metadata."""

    name = "example_transform"

    def apply(self, records, context):
        """
        Transform a batch of records.

        Args:
            records: List of record dicts
            context: Execution context (execution_id, runtime_params, etc.)

        Returns:
            Transformed list of records
        """
        execution_id = context.get("execution_id", "unknown")
        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id
        return records
