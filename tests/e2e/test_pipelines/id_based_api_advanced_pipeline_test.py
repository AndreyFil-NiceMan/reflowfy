"""
Advanced IDBasedAPISource E2E Test Pipelines.

Four pipeline classes covering all major IDBasedAPISource modes:

1. E2ERawListSearchPipeline   — batch POST, raw list body [1,2,3]
2. E2EPatchBulkPipeline       — PATCH with merged request_body fields
3. E2EPerIdPostPipeline       — per-ID POST with {id} body substitution
4. E2EProductsBatchPipeline   — custom batch_id_key + nested data_key
"""

from reflowfy import (
    IdBasedPipeline,
    PipelineParameter,
    transformation,
)
from tests.e2e.test_pipelines.shared_sources import e2e_id_based_api
from tests.e2e.test_pipelines.shared_destinations import e2e_console


# ============================================================================
# Transformations
# ============================================================================

@transformation("raw_list_tag_source")
def raw_list_tag_source(records, context):
    """Tag every record to confirm it came through the raw-list search endpoint."""
    for record in records:
        record["_source_endpoint"] = "POST /users/search"
        record["_batch_ids"] = context.get("current_ids", [])
    return records


@transformation("raw_list_count_records")
def raw_list_count_records(records, context):
    """Add a record-position index within the batch."""
    for i, record in enumerate(records):
        record["_batch_position"] = i
    return records


@transformation("patch_add_metadata")
def patch_add_metadata(records, context):
    """Stamp records that came from the PATCH /users/bulk endpoint."""
    for record in records:
        record["_source_endpoint"] = "PATCH /users/bulk"
        record["_active_only_filter"] = context.get("active_only", False)
    return records


@transformation("patch_compute_stats")
def patch_compute_stats(records, context):
    """Compute a simple stats field — count of active records in the batch."""
    active_count = sum(1 for r in records if r.get("active", False))
    for record in records:
        record["_active_in_batch"] = active_count
    return records


@transformation("per_id_verify_enrichment")
def per_id_verify_enrichment(records, context):
    """Verify that every record has the enrichment sub-object."""
    for record in records:
        if "enrichment" not in record:
            record["_enrichment_missing"] = True
        record["_enrichment_verified"] = "enrichment" in record
    return records


@transformation("products_tag_category")
def products_tag_category(records, context):
    """Add a category label for easy filtering downstream."""
    LABELS = {"A": "premium", "B": "standard", "C": "economy"}
    for record in records:
        cat = record.get("category", "")
        record["_category_label"] = LABELS.get(cat, "unknown")
    return records


@transformation("products_add_tax")
def products_add_tax(records, context):
    """Add a 10 % tax field to each product record."""
    for record in records:
        price = record.get("price", 0.0)
        record["price_with_tax"] = round(price * 1.10, 2)
    return records


# ============================================================================
# Pipeline 1: Raw List Body  (batch_id_key=None)
# ============================================================================

class E2ERawListSearchPipeline(IdBasedPipeline):
    """
    Sends user IDs to ``POST /users/search`` as a **raw JSON array** body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/search"``  — no ``{id}`` → batch mode
    - ``method="POST"``
    - ``batch_id_key=None``                  — body is ``[1,2,3,4,5]``
    - ``data_key="results"``                 — extract from response["results"]
    - ``batch_size=5``                       — 5 records per SourceJob

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per POST call
    """

    name = "e2e_raw_list_search_pipeline"
    rate_limit = {"jobs_per_second": 20}
    ids_batch_size = 5

    def define_parameters(self):
        return [
            PipelineParameter(
                name="batch_size",
                description="Records per SourceJob",
                param_type=int,
                required=False,
                default=5,
            ),
        ]

    def define_source(self, params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/search",
            ids=current_ids,
            method="POST",
            batch_id_key=None,         # raw list body: [1,2,3,4,5]
            data_key="results",
            batch_size=params.get("batch_size", 5),
        )

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            raw_list_tag_source(),
            raw_list_count_records(),
        ]


# ============================================================================
# Pipeline 2: PATCH Bulk with merged request_body
# ============================================================================

class E2EPatchBulkPipeline(IdBasedPipeline):
    """
    Sends user IDs to ``PATCH /users/bulk`` with an extra ``active_only`` field
    merged into the request body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/bulk"``     — no ``{id}`` → batch mode
    - ``method="PATCH"``
    - ``batch_id_key="ids"``                  — body: ``{"ids": [...], "active_only": <bool>}``
    - ``request_body={"active_only": ...}``   — merged alongside IDs
    - ``data_key="updated"``                  — extract from response["updated"]
    - ``batch_size=4``

    Pipeline config:
    - ``ids_batch_size=8``  — 8 IDs per PATCH call
    """

    name = "e2e_patch_bulk_pipeline"
    rate_limit = {"jobs_per_second": 20}
    ids_batch_size = 8

    def define_parameters(self):
        return [
            PipelineParameter(
                name="active_only",
                description="When true, only active users are returned from the bulk endpoint",
                param_type=bool,
                required=False,
                default=False,
            ),
            PipelineParameter(
                name="batch_size",
                description="Records per SourceJob",
                param_type=int,
                required=False,
                default=4,
            ),
        ]

    def define_source(self, params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/bulk",
            ids=current_ids,
            method="PATCH",
            batch_id_key="ids",
            request_body={"active_only": params.get("active_only", False)},
            data_key="updated",
            batch_size=params.get("batch_size", 4),
        )

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            patch_add_metadata(),
            patch_compute_stats(),
        ]


# ============================================================================
# Pipeline 3: Per-ID POST with {id} body substitution
# ============================================================================

class E2EPerIdPostPipeline(IdBasedPipeline):
    """
    Calls ``POST /users/{id}/enrich`` individually for each user ID.

    IDBasedAPISource config:
    - ``endpoint_template="/users/{id}/enrich"`` — ``{id}`` present → per-ID mode
    - ``method="POST"``
    - ``request_body={"context": "e2e_test", "source_id": "{id}"}``
      → each request body gets the actual ID substituted for ``{id}``
    - ``batch_size=5``  — 5 per-ID responses grouped into one SourceJob

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per define_source call
    """

    name = "e2e_per_id_post_pipeline"
    rate_limit = {"jobs_per_second": 20}
    ids_batch_size = 5

    def define_parameters(self):
        return [
            PipelineParameter(
                name="batch_size",
                description="IDs grouped per SourceJob",
                param_type=int,
                required=False,
                default=5,
            ),
        ]

    def define_source(self, params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/{id}/enrich",   # per-ID mode
            ids=current_ids,
            method="POST",
            request_body={"context": "e2e_test", "source_id": "{id}"},
            batch_size=params.get("batch_size", 5),
        )

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            per_id_verify_enrichment(),
        ]


# ============================================================================
# Pipeline 4: Products batch with custom batch_id_key
# ============================================================================

class E2EProductsBatchPipeline(IdBasedPipeline):
    """
    Looks up products from ``POST /products/lookup`` using a non-default
    body key (``product_ids`` instead of ``ids``).

    IDBasedAPISource config:
    - ``endpoint_template="/products/lookup"``  — no ``{id}`` → batch mode
    - ``method="POST"``
    - ``batch_id_key="product_ids"``            — body: ``{"product_ids": [...]}``
    - ``data_key="items"``
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=10``  — 10 product IDs per POST call
    """

    name = "e2e_products_batch_pipeline"
    rate_limit = {"jobs_per_second": 20}
    ids_batch_size = 10

    def define_parameters(self):
        return [
            PipelineParameter(
                name="batch_size",
                description="Records per SourceJob",
                param_type=int,
                required=False,
                default=5,
            ),
        ]

    def define_source(self, params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/products/lookup",
            ids=current_ids,
            method="POST",
            batch_id_key="product_ids",    # custom key → {"product_ids": [...]}
            data_key="items",
            batch_size=params.get("batch_size", 5),
        )

    def define_destination(self, params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, params, current_ids):
        return [
            products_tag_category(),
            products_add_tax(),
        ]
