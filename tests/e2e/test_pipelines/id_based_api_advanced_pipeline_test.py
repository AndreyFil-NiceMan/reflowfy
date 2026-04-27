"""
Advanced IDBasedAPISource E2E Test Pipelines.

Four pipeline classes covering all major IDBasedAPISource modes:

1. E2ERawListSearchPipeline  — batch POST, raw list body [1,2,3]
2. E2EPatchBulkPipeline      — PATCH with merged request_body fields
3. E2EPerIdPostPipeline      — per-ID POST with {id} body substitution
4. E2EProductsBatchPipeline  — custom batch_id_key + nested data_key
"""

from reflowfy import IdBasedPipeline, PipelineParameter
from tests.e2e.test_pipelines.destinations import e2e_console
from tests.e2e.test_pipelines.sources import e2e_id_based_api
from tests.e2e.test_pipelines.transformations import (
    patch_add_metadata,
    patch_compute_stats,
    per_id_verify_enrichment,
    products_add_tax,
    products_tag_category,
    raw_list_count_records,
    raw_list_tag_source,
)


class E2ERawListSearchPipeline(IdBasedPipeline):
    """
    Sends user IDs to ``POST /users/search`` as a **raw JSON array** body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/search"``  — no ``{id}`` → batch mode
    - ``method="POST"``
    - ``batch_id_key=None``                  — body is ``[1,2,3,4,5]``
    - ``data_key="results"``                 — extract from response["results"]
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per POST call
    """

    name = "e2e_raw_list_search_pipeline"
    rate_limit = 20
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

    def define_source(self, runtime_params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/search",
            ids=current_ids,
            method="POST",
            batch_id_key=None,
            data_key="results",
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(self, runtime_params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, runtime_params, current_ids):
        return [
            raw_list_tag_source(),
            raw_list_count_records(),
        ]


class E2EPatchBulkPipeline(IdBasedPipeline):
    """
    Sends user IDs to ``PATCH /users/bulk`` with an extra ``active_only`` field
    merged into the request body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/bulk"``     — no ``{id}`` → batch mode
    - ``method="PATCH"``
    - ``batch_id_key="ids"``                  — body: ``{"ids": [...], "active_only": <bool>}``
    - ``data_key="updated"``                  — extract from response["updated"]
    - ``batch_size=4``

    Pipeline config:
    - ``ids_batch_size=8``  — 8 IDs per PATCH call
    """

    name = "e2e_patch_bulk_pipeline"
    rate_limit = 20
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

    def define_source(self, runtime_params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/bulk",
            ids=current_ids,
            method="PATCH",
            batch_id_key="ids",
            request_body={"active_only": runtime_params.get("active_only", False)},
            data_key="updated",
            batch_size=runtime_params.get("batch_size", 4),
        )

    def define_destination(self, runtime_params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, runtime_params, current_ids):
        return [
            patch_add_metadata(),
            patch_compute_stats(),
        ]


class E2EPerIdPostPipeline(IdBasedPipeline):
    """
    Calls ``POST /users/{id}/enrich`` individually for each user ID.

    IDBasedAPISource config:
    - ``endpoint_template="/users/{id}/enrich"`` — ``{id}`` present → per-ID mode
    - ``method="POST"``
    - ``request_body={"context": "e2e_test", "source_id": "{id}"}``
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per define_source call
    """

    name = "e2e_per_id_post_pipeline"
    rate_limit = 20
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

    def define_source(self, runtime_params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/users/{id}/enrich",
            ids=current_ids,
            method="POST",
            request_body={"context": "e2e_test", "source_id": "{id}"},
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(self, runtime_params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, runtime_params, current_ids):
        return [per_id_verify_enrichment()]


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
    rate_limit = 20
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

    def define_source(self, runtime_params, current_ids):
        return e2e_id_based_api(
            endpoint_template="/products/lookup",
            ids=current_ids,
            method="POST",
            batch_id_key="product_ids",
            data_key="items",
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(self, runtime_params):
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(self, runtime_params, current_ids):
        return [
            products_tag_category(),
            products_add_tax(),
        ]
