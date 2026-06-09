"""
Reusable transformation definitions for E2E test pipelines.

All transformations use the @transformation decorator.
Import any transformation from this module and instantiate it in define_transformations().

The second argument to every transformation function is runtime_params — a flat
mutable dict that merges user-supplied pipeline parameters with execution-context
keys (execution_id, batch_id, pipeline_name, created_at, current_ids for id-based
pipelines). Any keys written into runtime_params are visible to subsequent
transformations and to the destination within the same job execution.
"""

from datetime import datetime, timezone

from reflowfy import transformation

# ---------------------------------------------------------------------------
# Common / shared transforms
# ---------------------------------------------------------------------------


@transformation("transform_add_timestamp")
def transform_add_timestamp(records, runtime_params):
    """Adds a processing timestamp and source marker."""
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        record["_execution_id"] = execution_id
        record["_transform_step_1"] = True
    return records


@transformation("transform_enrich_record")
def transform_enrich_record(records, runtime_params):
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
def crash_recovery_add_info(records, runtime_params):
    """Adds crash-recovery pipeline metadata to records."""
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_test_pipeline"] = "crash_recovery"
        record["_execution_id"] = execution_id
    return records


@transformation("rl_passthrough")
def rl_passthrough(records, runtime_params):
    """Stamps dispatch timestamp and batch_id for rate-limit timing tests."""
    ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    for record in records:
        record["_rl_dispatched_at"] = ts
        record["_rl_batch_id"] = runtime_params.get("batch_id", "")
    return records


# ---------------------------------------------------------------------------
# Execution context probing
# ---------------------------------------------------------------------------


@transformation("ctx_probe")
def ctx_probe(records, runtime_params):
    """Stamps all four ExecutionContext keys onto each record."""
    for record in records:
        record["_ctx_execution_id"] = runtime_params.get("execution_id", "")
        record["_ctx_batch_id"] = runtime_params.get("batch_id", "")
        record["_ctx_pipeline_name"] = runtime_params.get("pipeline_name", "")
        record["_ctx_created_at"] = runtime_params.get("created_at", "")
    return records


@transformation("ctx_runtime_params")
def ctx_runtime_params(records, runtime_params):
    """Reads env and multiplier from runtime_params; computes _value = id * multiplier."""
    env = runtime_params.get("env", "default")
    multiplier = int(runtime_params.get("multiplier", 1))
    for record in records:
        record["_env"] = env
        record["_value"] = record.get("id", 0) * multiplier
    return records


@transformation("ctx_enrich")
def ctx_enrich(records, runtime_params):
    """Step 1 of 2: marks records as enriched."""
    for record in records:
        record["_enriched"] = True
    return records


@transformation("ctx_maybe_fail")
def ctx_maybe_fail(records, runtime_params):
    """Step 2 of 2: raises TransformationError only for id==999 (never in mock data)."""
    from reflowfy.transformations.base import TransformationError

    for record in records:
        if record.get("id") == 999:
            raise TransformationError("ctx_maybe_fail", "Intentional failure for id=999", None)
        record["_step2_done"] = True
    return records


@transformation("ctx_batch_id")
def ctx_batch_id(records, runtime_params):
    """Stamps the current batch_id onto each record for cross-batch uniqueness testing."""
    bid = runtime_params.get("batch_id", "")
    for record in records:
        record["_batch_id"] = bid
    return records


# ---------------------------------------------------------------------------
# Source metadata
# ---------------------------------------------------------------------------


@transformation("add_source_info")
def add_source_info(records, runtime_params):
    """Adds Elasticsearch source metadata to records."""
    for record in records:
        record["_source_type"] = "elasticsearch"
        record["_test_pipeline"] = "elastic_source_test"
    return records


@transformation("elastic_add_metadata_and_route")
def elastic_add_metadata_and_route(records, runtime_params):
    """Adds per-document metadata and a per-job route hint for destination selection."""
    page_num = int(runtime_params.get("page_num", 0))
    route_target = "primary" if page_num % 2 == 0 else "secondary"
    execution_id = runtime_params.get("execution_id", "unknown")

    for record in records:
        record["_source_type"] = "elasticsearch"
        record["_test_pipeline"] = "elastic_routed_destinations"
        record["_execution_id"] = execution_id
        record["_page_num"] = page_num
        record["_event_type"] = record.get("event_type", "unknown")
        record["_has_amount"] = record.get("amount") is not None
        record["_route_target"] = route_target

    return records


@transformation("sql_add_source_info")
def sql_add_source_info(records, runtime_params):
    """Adds SQL source metadata to records."""
    for record in records:
        record["_source_type"] = "sql"
        record["_test_pipeline"] = "sql_source_test"
    return records


@transformation("sql_filter_by_status")
def sql_filter_by_status(records, runtime_params):
    """Filters records by status from runtime_params."""
    status_filter = runtime_params.get("filter_status", "active")
    filtered = [r for r in records if r.get("status") == status_filter]
    print(f"  📊 SQL Filter: {len(records)} → {len(filtered)} records (status={status_filter})")
    return filtered


@transformation("api_add_source_info")
def api_add_source_info(records, runtime_params):
    """Adds API source metadata to records."""
    for record in records:
        record["_source_type"] = "api"
        record["_test_pipeline"] = "e2e_api_source_test"
    return records


@transformation("api_log_record_count")
def api_log_record_count(records, runtime_params):
    """Logs the number of records processed from the API source."""
    print(f"  📊 API Source: Processing {len(records)} records")
    return records


@transformation("api_id_add_source_info")
def api_id_add_source_info(records, runtime_params):
    """Adds ID-based API source metadata to records."""
    for record in records:
        record["_source_type"] = "api_id"
        record["_test_pipeline"] = "e2e_api_id_source_test"
    return records


@transformation("api_id_log_record_count")
def api_id_log_record_count(records, runtime_params):
    """Logs the number of records processed from the ID-based API source."""
    print(f"  📊 ID-Based API Source: Processing {len(records)} records")
    return records


# ---------------------------------------------------------------------------
# Destination metadata
# ---------------------------------------------------------------------------


@transformation("api_add_dest_info")
def api_add_dest_info(records, runtime_params):
    """Adds API destination metadata to records."""
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "api"
        record["_test_pipeline"] = "api_dest_test"
        record["_execution_id"] = execution_id
    return records


@transformation("kafka_add_dest_info")
def kafka_add_dest_info(records, runtime_params):
    """Adds Kafka destination metadata to records."""
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_destination_type"] = "kafka"
        record["_test_pipeline"] = "kafka_dest_test"
        record["_execution_id"] = execution_id
    return records


# ---------------------------------------------------------------------------
# IdBasedPipeline transforms
# ---------------------------------------------------------------------------


@transformation("id_pipeline_add_metadata")
def id_pipeline_add_metadata(records, runtime_params):
    """Stamps current_ids and execution metadata onto each record."""
    current_ids = runtime_params.get("current_ids", [])
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_processed_by_id_pipeline"] = True
        record["_current_ids"] = current_ids
        record["_execution_id"] = execution_id
    return records


@transformation("id_pipeline_enrich")
def id_pipeline_enrich(records, runtime_params):
    """Enriches records with computed fields based on ID processing."""
    for record in records:
        record["_id_pipeline_verified"] = record.get("_processed_by_id_pipeline", False)
        record["_test_pipeline"] = "e2e_id_based_pipeline_test"
    return records


# ---------------------------------------------------------------------------
# Advanced IDBasedAPISource transforms
# ---------------------------------------------------------------------------


@transformation("raw_list_tag_source")
def raw_list_tag_source(records, runtime_params):
    """Tags records to confirm they came through the raw-list search endpoint."""
    for record in records:
        record["_source_endpoint"] = "POST /users/search"
        record["_batch_ids"] = runtime_params.get("current_ids", [])
    return records


@transformation("raw_list_count_records")
def raw_list_count_records(records, runtime_params):
    """Adds a record-position index within the batch."""
    for i, record in enumerate(records):
        record["_batch_position"] = i
    return records


@transformation("patch_add_metadata")
def patch_add_metadata(records, runtime_params):
    """Stamps records that came from the PATCH /users/bulk endpoint."""
    for record in records:
        record["_source_endpoint"] = "PATCH /users/bulk"
        record["_active_only_filter"] = runtime_params.get("active_only", False)
    return records


@transformation("patch_compute_stats")
def patch_compute_stats(records, runtime_params):
    """Computes count of active records in the batch."""
    active_count = sum(1 for r in records if r.get("active", False))
    for record in records:
        record["_active_in_batch"] = active_count
    return records


@transformation("per_id_verify_enrichment")
def per_id_verify_enrichment(records, runtime_params):
    """Verifies that every record has the enrichment sub-object."""
    for record in records:
        if "enrichment" not in record:
            record["_enrichment_missing"] = True
        record["_enrichment_verified"] = "enrichment" in record
    return records


@transformation("products_tag_category")
def products_tag_category(records, runtime_params):
    """Adds a category label for easy downstream filtering."""
    LABELS = {"A": "premium", "B": "standard", "C": "economy"}
    for record in records:
        cat = record.get("category", "")
        record["_category_label"] = LABELS.get(cat, "unknown")
    return records


@transformation("products_add_tax")
def products_add_tax(records, runtime_params):
    """Adds a 10% tax field to each product record."""
    for record in records:
        price = record.get("price", 0.0)
        record["price_with_tax"] = round(price * 1.10, 2)
    return records


@transformation("api_batch_add_metadata")
def api_batch_add_metadata(records, runtime_params):
    """Stamps each record with the IDs batch and execution context."""
    current_ids = runtime_params.get("current_ids", [])
    execution_id = runtime_params.get("execution_id", "unknown")
    for record in records:
        record["_batch_ids"] = current_ids
        record["_execution_id"] = execution_id
        record["_source"] = "api_batch_post"
    return records


@transformation("api_batch_filter_active")
def api_batch_filter_active(records, runtime_params):
    """Keeps only active users and adds a computed display_name field."""
    result = []
    for record in records:
        if record.get("active", True):
            record["display_name"] = f"{record.get('name', '')} <{record.get('email', '')}>"
            result.append(record)
    return result


# ---------------------------------------------------------------------------
# runtime_params enrichment transforms (for e2e enrichment tests)
# ---------------------------------------------------------------------------


@transformation("params_step1_enrich")
def params_step1_enrich(records, runtime_params):
    """Step 1: writes step1_count into runtime_params for the next transform."""
    runtime_params["step1_count"] = len(records)
    runtime_params["step1_ran"] = True
    injected = runtime_params.get("injected_by_source", "")
    for record in records:
        record["_step1"] = True
        record["_injected_by_source"] = injected
        record["_saw_step1_count"] = runtime_params["step1_count"]
        record["_saw_step1_ran"] = runtime_params["step1_ran"]
        record["_saw_injected"] = injected
        # Mirror fields without leading underscore for environments that
        # sanitize specific key prefixes in downstream test tooling.
        record["saw_step1_count"] = runtime_params["step1_count"]
        record["saw_step1_ran"] = runtime_params["step1_ran"]
        record["saw_injected"] = injected
    return records


@transformation("reveal_set_flag")
def reveal_set_flag(records, runtime_params):
    """Mid-chain reveal step 1: sets a flag in runtime_params that the pipeline's
    define_transformations uses to append a second transformation on the next pass."""
    runtime_params["should_add_second"] = True
    for record in records:
        record["_reveal_step1"] = True
    return records


@transformation("reveal_stamp_second")
def reveal_stamp_second(records, runtime_params):
    """Mid-chain reveal step 2: only appended (and thus only runs) when step 1 set
    the flag. Stamps every record so the test can assert it actually ran."""
    for record in records:
        record["second_applied"] = True
    return records


@transformation("params_step2_verify")
def params_step2_verify(records, runtime_params):
    """Step 2: verifies it can read what step1 wrote into runtime_params."""
    step1_count = runtime_params.get("step1_count", -1)
    step1_ran = runtime_params.get("step1_ran", False)
    injected = runtime_params.get("injected_by_source", "")
    execution_id = runtime_params.get("execution_id", "")
    for record in records:
        record["_saw_step1_count"] = step1_count
        record["_saw_step1_ran"] = step1_ran
        record["_saw_injected"] = injected
        # Mirror fields without leading underscore for environments that
        # sanitize specific key prefixes in downstream test tooling.
        record["saw_step1_count"] = step1_count
        record["saw_step1_ran"] = step1_ran
        record["saw_injected"] = injected
        record["_execution_id"] = execution_id
        record["_step2"] = True
    return records
