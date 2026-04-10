"""
Advanced Transformation Test Pipelines.

Four pipelines for testing transformation context features:
- E2EContextProbePipeline: stamps all ExecutionContext keys onto each record
- E2ERuntimeParamsPipeline: reads runtime_params from context and computes values
- E2EErrorTolerantPipeline: two-step chain; id==999 would raise but never appears
- E2EBatchIdentityPipeline: stamps batch_id per record to verify uniqueness per batch
"""

from reflowfy import AbstractPipeline, transformation
from tests.e2e.test_pipelines.shared_sources import e2e_mock
from tests.e2e.test_pipelines.shared_destinations import e2e_http


@transformation("ctx_probe")
def ctx_probe(records, context):
    """Stamp every ExecutionContext key onto each record."""
    for record in records:
        record["_ctx_execution_id"] = context.get("execution_id", "")
        record["_ctx_batch_id"] = context.get("batch_id", "")
        record["_ctx_pipeline_name"] = context.get("pipeline_name", "")
        record["_ctx_created_at"] = context.get("created_at", "")
    return records


@transformation("ctx_runtime_params")
def ctx_runtime_params(records, context):
    """Read env and multiplier from runtime_params; compute _value = id * multiplier."""
    runtime = context.get("runtime_params", {})
    env = runtime.get("env", "default")
    multiplier = int(runtime.get("multiplier", 1))
    for record in records:
        record["_env"] = env
        record["_value"] = record.get("id", 0) * multiplier
    return records


@transformation("ctx_enrich")
def ctx_enrich(records, context):
    """Step 1 of 2: mark records as enriched."""
    for record in records:
        record["_enriched"] = True
    return records


@transformation("ctx_maybe_fail")
def ctx_maybe_fail(records, context):
    """Step 2 of 2: raises TransformationError only for id==999 (never in mock data)."""
    from reflowfy.transformations.base import TransformationError

    for record in records:
        if record.get("id") == 999:
            raise TransformationError("ctx_maybe_fail", "Intentional failure for id=999", None)
        record["_step2_done"] = True
    return records


@transformation("ctx_batch_id")
def ctx_batch_id(records, context):
    """Stamp the current batch_id onto each record for cross-batch uniqueness testing."""
    bid = context.get("batch_id", "")
    for record in records:
        record["_batch_id"] = bid
    return records


class E2EContextProbePipeline(AbstractPipeline):
    """Stamps all 4 ExecutionContext keys onto records."""

    name = "e2e_context_probe"
    rate_limit = {"jobs_per_second": 50}

    def define_source(self, params):
        return e2e_mock(count=20, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [ctx_probe()]


class E2ERuntimeParamsPipeline(AbstractPipeline):
    """Reads runtime_params; test sends env=staging and multiplier=3."""

    name = "e2e_runtime_params"
    rate_limit = {"jobs_per_second": 50}

    def define_source(self, params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [ctx_runtime_params()]


class E2EErrorTolerantPipeline(AbstractPipeline):
    """Two-step transformation chain; mock data never contains id=999 so always completes."""

    name = "e2e_error_tolerant"
    rate_limit = {"jobs_per_second": 50}

    def define_source(self, params):
        return e2e_mock(count=10, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [ctx_enrich(), ctx_maybe_fail()]


class E2EBatchIdentityPipeline(AbstractPipeline):
    """30 records across 3 batches of 10; each batch must carry a distinct batch_id."""

    name = "e2e_batch_identity"
    rate_limit = {"jobs_per_second": 50}

    def define_source(self, params):
        return e2e_mock(count=30, batch_size=10)

    def define_destination(self, params):
        return e2e_http()

    def define_transformations(self, params):
        return [ctx_batch_id()]
