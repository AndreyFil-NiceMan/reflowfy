"""
Example reusable transformation.

Transformations auto-register via metaclass when they subclass BaseTransformation.
Just define the class and import it in any pipeline.
"""

from typing import Any, Dict, List
from reflowfy import BaseTransformation


class ExampleTransform(BaseTransformation):
    """Example transformation that adds processing metadata."""

    name = "example_transform"

    def apply(self, records: List[Any], runtime_params: Dict[str, Any]) -> List[Any]:
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
        for record in records:
            record["_processed_by"] = "reflowfy"
            record["_execution_id"] = execution_id
        return records
