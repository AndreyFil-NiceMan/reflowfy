"""
Reusable transformation definitions for E2E test pipelines.

All transformations use the @transformation decorator.
Import any transformation from this module and instantiate it in define_transformations().
"""

from datetime import datetime

from reflowfy import transformation


# ---------------------------------------------------------------------------
# Common / shared transforms
# ---------------------------------------------------------------------------

@transformation("transform_add_timestamp")
def transform_add_timestamp(records, context):
    """Adds a processing timestamp and source marker."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_processed_at"] = datetime.utcnow().isoformat()
        record["_execution_id"] = execution_id
        record["_transform_step_1"] = True
    return records


@transformation("transform_enrich_record")
def transform_enrich_record(records, context):
    """Enriches records with computed fields; verifies step-1 ran first."""
    for record in records:
        record["_transform_step_2"] = True
        record["_transform_chain_verified"] = record.get("_transform_step_1", False)
        record_id = record.get("id", 0)
        record["_computed_category"] = "even" if record_id % 2 == 0 else "odd"
        record["_destination_type"] = "http"
        record["_test_pipeline"] = "transformation_verify"
    return records


@transformation("crash_recovery_add_info")
def crash_recovery_add_info(records, context):
    """Adds crash-recovery pipeline metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_test_pipeline"] = "crash_recovery"
        record["_execution_id"] = execution_id
    return records


@transformation("rl_passthrough")
def rl_passthrough(records, context):
    """Stamps dispatch timestamp and batch_id for rate-limit timing tests."""
    ts = datetime.utcnow().isoformat()
    for record in records:
        record["_rl_dispatched_at"] = ts
        record["_rl_batch_id"] = context.get("batch_id", "")
    return records


# ---------------------------------------------------------------------------
# Execution context probing
# ---------------------------------------------------------------------------

@transformation("ctx_probe")
def ctx_probe(records, context):
    """Stamps all four ExecutionContext keys onto each record."""
    for record in records:
        record["_ctx_execution_id"] = context.get("execution_id", "")
        record["_ctx_batch_id"] = context.get("batch_id", "")
        record["_ctx_pipeline_name"] = context.get("pipeline_name", "")
        record["_ctx_created_at"] = context.get("created_at", "")
    return records


@transformation("ctx_runtime_params")
def ctx_runtime_params(records, context):
    """Reads env and multiplier from runtime_params; computes _value = id * multiplier."""
    runtime = context.get("runtime_params", {})
    env = runtime.get("env", "default")
    multiplier = int(runtime.get("multiplier", 1))
    for record in records:
        record["_env"] = env
        record["_value"] = record.get("id", 0) * multiplier
    return records


@transformation("ctx_enrich")
def ctx_enrich(records, context):
    """Step 1 of 2: marks records as enriched."""
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
    """Stamps the current batch_id onto each record for cross-batch uniqueness testing."""
    bid = context.get("batch_id", "")
    for record in records:
        record["_batch_id"] = bid
    return records


# ---------------------------------------------------------------------------
# Source metadata
# ---------------------------------------------------------------------------

@transformation("add_source_info")
def add_source_info(records, context):
    """Adds Elasticsearch source metadata to records."""
    for record in records:
        record["_source_type"] = "elasticsearch"
        record["_test_pipeline"] = "elastic_source_test"
    return records


@transformation("sql_add_source_info")
def sql_add_source_info(records, context):
    """Adds SQL source metadata to records."""
    for record in records:
        record["_source_type"] = "sql"
        record["_test_pipeline"] = "sql_source_test"
    return records


@transformation("sql_filter_by_status")
def sql_filter_by_status(records, context):
    """Filters records by status from runtime_params."""
    status_filter = context.get("runtime_params", {}).get("filter_status", "active")
    filtered = [r for r in records if r.get("status") == status_filter]
    print(f"  📊 SQL Filter: {len(records)} → {len(filtered)} records (status={status_filter})")
    return filtered


@transformation("api_add_source_info")
def api_add_source_info(records, context):
    """Adds paginated API source metadata to records."""
    for record in records:
        record["_source_type"] = "api"
        record["_test_pipeline"] = "e2e_api_source_test"
    return records


@transformation("api_log_record_count")
def api_log_record_count(records, context):
    """Logs the number of records processed from the API source."""
    print(f"  📊 API Source: Processing {len(records)} records")
    return records


@transformation("api_id_add_source_info")
def api_id_add_source_info(records, context):
    """Adds ID-based API source metadata to records."""
    for record in records:
        record["_source_type"] = "api_id"
        record["_test_pipeline"] = "e2e_api_id_source_test"
    return records


@transformation("api_id_log_record_count")
def api_id_log_record_count(records, context):
    """Logs the number of records processed from the ID-based API source."""
    print(f"  📊 ID-Based API Source: Processing {len(records)} records")
    return records


# ---------------------------------------------------------------------------
# Destination metadata
# ---------------------------------------------------------------------------

@transformation("http_add_dest_info")
def http_add_dest_info(records, context):
    """Adds HTTP destination metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "http"
        record["_test_pipeline"] = "http_dest_test"
        record["_execution_id"] = execution_id
    return records


@transformation("kafka_add_dest_info")
def kafka_add_dest_info(records, context):
    """Adds Kafka destination metadata to records."""
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "kafka"
        record["_test_pipeline"] = "kafka_dest_test"
        record["_execution_id"] = execution_id
    return records


# ---------------------------------------------------------------------------
# IdBasedPipeline transforms
# ---------------------------------------------------------------------------

@transformation("id_pipeline_add_metadata")
def id_pipeline_add_metadata(records, context):
    """Stamps current_ids and execution metadata onto each record."""
    current_ids = context.get("current_ids", [])
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_processed_by_id_pipeline"] = True
        record["_current_ids"] = current_ids
        record["_execution_id"] = execution_id
    return records


@transformation("id_pipeline_enrich")
def id_pipeline_enrich(records, context):
    """Enriches records with computed fields based on ID processing."""
    for record in records:
        record["_id_pipeline_verified"] = record.get("_processed_by_id_pipeline", False)
        record["_test_pipeline"] = "e2e_id_based_pipeline_test"
    return records


# ---------------------------------------------------------------------------
# Advanced IDBasedAPISource transforms
# ---------------------------------------------------------------------------

@transformation("raw_list_tag_source")
def raw_list_tag_source(records, context):
    """Tags records to confirm they came through the raw-list search endpoint."""
    for record in records:
        record["_source_endpoint"] = "POST /users/search"
        record["_batch_ids"] = context.get("current_ids", [])
    return records


@transformation("raw_list_count_records")
def raw_list_count_records(records, context):
    """Adds a record-position index within the batch."""
    for i, record in enumerate(records):
        record["_batch_position"] = i
    return records


@transformation("patch_add_metadata")
def patch_add_metadata(records, context):
    """Stamps records that came from the PATCH /users/bulk endpoint."""
    for record in records:
        record["_source_endpoint"] = "PATCH /users/bulk"
        record["_active_only_filter"] = context.get("active_only", False)
    return records


@transformation("patch_compute_stats")
def patch_compute_stats(records, context):
    """Computes count of active records in the batch."""
    active_count = sum(1 for r in records if r.get("active", False))
    for record in records:
        record["_active_in_batch"] = active_count
    return records


@transformation("per_id_verify_enrichment")
def per_id_verify_enrichment(records, context):
    """Verifies that every record has the enrichment sub-object."""
    for record in records:
        if "enrichment" not in record:
            record["_enrichment_missing"] = True
        record["_enrichment_verified"] = "enrichment" in record
    return records


@transformation("products_tag_category")
def products_tag_category(records, context):
    """Adds a category label for easy downstream filtering."""
    LABELS = {"A": "premium", "B": "standard", "C": "economy"}
    for record in records:
        cat = record.get("category", "")
        record["_category_label"] = LABELS.get(cat, "unknown")
    return records


@transformation("products_add_tax")
def products_add_tax(records, context):
    """Adds a 10% tax field to each product record."""
    for record in records:
        price = record.get("price", 0.0)
        record["price_with_tax"] = round(price * 1.10, 2)
    return records


@transformation("api_batch_add_metadata")
def api_batch_add_metadata(records, context):
    """Stamps each record with the IDs batch and execution context."""
    current_ids = context.get("current_ids", [])
    execution_id = context.get("execution_id", "unknown")
    for record in records:
        record["_batch_ids"] = current_ids
        record["_execution_id"] = execution_id
        record["_source"] = "api_batch_post"
    return records


@transformation("api_batch_filter_active")
def api_batch_filter_active(records, context):
    """Keeps only active users and adds a computed display_name field."""
    result = []
    for record in records:
        if record.get("active", True):
            record["display_name"] = f"{record.get('name', '')} <{record.get('email', '')}>"
            result.append(record)
    return result
