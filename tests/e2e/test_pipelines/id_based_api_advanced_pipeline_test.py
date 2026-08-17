"""
Advanced IDBasedAPISource E2E Test Pipelines.

Four pipeline classes covering all major IDBasedAPISource modes:

1. E2ERawListSearchPipeline  — batch POST, raw list body (body=<ids>)
2. E2EPatchBulkPipeline      — PATCH with merged body fields (body={"ids": <ids>, "active_only": <x>})
3. E2EPerIdPostPipeline      — per-ID POST with {id} body substitution
4. E2EProductsBatchPipeline  — custom body key + nested response_key (body={"product_ids": <ids>})
"""

from typing import Any, List, Sequence

from typing_extensions import Annotated, NotRequired

from reflowfy import IdBasedPipeline, Param, RuntimeParams
from reflowfy.destinations.base import BaseDestination
from reflowfy.sources.base import BaseSource
from reflowfy.transformations.base import BaseTransformation
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


class RawListSearchParams(RuntimeParams, total=False):
    """Parameters for :class:`E2ERawListSearchPipeline`."""

    batch_size: Annotated[NotRequired[int], Param("Records per SourceJob", default=5)]


class E2ERawListSearchPipeline(IdBasedPipeline[RawListSearchParams]):
    """
    Sends user IDs to ``POST /users/search`` as a **raw JSON array** body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/search"``  — no ``{id}`` → batch mode
    - ``method="POST"``
    - ``body=<ids>``                         — body is ``[1,2,3,4,5]`` (raw list)
    - ``response_key="results"``             — extract from response["results"]
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per POST call
    """

    name = "e2e_raw_list_search_pipeline"
    rate_limit = 1200  # jobs per minute
    ids_batch_size = 5

    # define_parameters() is derived from RawListSearchParams.

    def define_source(self, runtime_params: RawListSearchParams) -> BaseSource:
        current_ids = runtime_params.get("current_ids", [])
        return e2e_id_based_api(
            endpoint_template="/users/search",
            ids=current_ids,
            method="POST",
            body=current_ids,
            response_key="results",
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(
        self, records: List[Any], runtime_params: RawListSearchParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(
        self, records: List[Any], runtime_params: RawListSearchParams
    ) -> Sequence[BaseTransformation]:
        return [
            raw_list_tag_source(),
            raw_list_count_records(),
        ]


class PatchBulkParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EPatchBulkPipeline`."""

    active_only: Annotated[
        NotRequired[bool],
        Param("When true, only active users are returned from the bulk endpoint", default=False),
    ]
    batch_size: Annotated[NotRequired[int], Param("Records per SourceJob", default=4)]


class E2EPatchBulkPipeline(IdBasedPipeline[PatchBulkParams]):
    """
    Sends user IDs to ``PATCH /users/bulk`` with an extra ``active_only`` field
    merged into the request body.

    IDBasedAPISource config:
    - ``endpoint_template="/users/bulk"``     — no ``{id}`` → batch mode
    - ``method="PATCH"``
    - ``body={"ids": <ids>, "active_only": <bool>}``  — verbatim body
    - ``response_key="updated"``             — extract from response["updated"]
    - ``batch_size=4``

    Pipeline config:
    - ``ids_batch_size=8``  — 8 IDs per PATCH call
    """

    name = "e2e_patch_bulk_pipeline"
    rate_limit = 1200  # jobs per minute
    ids_batch_size = 8

    # define_parameters() is derived from PatchBulkParams.

    def define_source(self, runtime_params: PatchBulkParams) -> BaseSource:
        current_ids = runtime_params.get("current_ids", [])
        return e2e_id_based_api(
            endpoint_template="/users/bulk",
            ids=current_ids,
            method="PATCH",
            body={"ids": current_ids, "active_only": runtime_params.get("active_only", False)},
            response_key="updated",
            batch_size=runtime_params.get("batch_size", 4),
        )

    def define_destination(
        self, records: List[Any], runtime_params: PatchBulkParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(
        self, records: List[Any], runtime_params: PatchBulkParams
    ) -> Sequence[BaseTransformation]:
        return [
            patch_add_metadata(),
            patch_compute_stats(),
        ]


class PerIdPostParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EPerIdPostPipeline`."""

    batch_size: Annotated[NotRequired[int], Param("IDs grouped per SourceJob", default=5)]


class E2EPerIdPostPipeline(IdBasedPipeline[PerIdPostParams]):
    """
    Calls ``POST /users/{id}/enrich`` individually for each user ID.

    IDBasedAPISource config:
    - ``endpoint_template="/users/{id}/enrich"`` — ``{id}`` present → per-ID mode
    - ``method="POST"``
    - ``body={"context": "e2e_test", "source_id": "{id}"}``
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=5``  — 5 IDs per define_source call
    """

    name = "e2e_per_id_post_pipeline"
    rate_limit = 1200  # jobs per minute
    ids_batch_size = 5

    # define_parameters() is derived from PerIdPostParams.

    def define_source(self, runtime_params: PerIdPostParams) -> BaseSource:
        current_ids = runtime_params.get("current_ids", [])
        return e2e_id_based_api(
            endpoint_template="/users/{id}/enrich",
            ids=current_ids,
            method="POST",
            body={"context": "e2e_test", "source_id": "{id}"},
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(
        self, records: List[Any], runtime_params: PerIdPostParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(
        self, records: List[Any], runtime_params: PerIdPostParams
    ) -> Sequence[BaseTransformation]:
        return [per_id_verify_enrichment()]


class ProductsBatchParams(RuntimeParams, total=False):
    """Parameters for :class:`E2EProductsBatchPipeline`."""

    batch_size: Annotated[NotRequired[int], Param("Records per SourceJob", default=5)]


class E2EProductsBatchPipeline(IdBasedPipeline[ProductsBatchParams]):
    """
    Looks up products from ``POST /products/lookup`` using a non-default
    body key (``product_ids`` instead of ``ids``).

    IDBasedAPISource config:
    - ``endpoint_template="/products/lookup"``  — no ``{id}`` → batch mode
    - ``method="POST"``
    - ``body={"product_ids": <ids>}``           — verbatim body with product_ids key
    - ``response_key="items"``
    - ``batch_size=5``

    Pipeline config:
    - ``ids_batch_size=10``  — 10 product IDs per POST call
    """

    name = "e2e_products_batch_pipeline"
    rate_limit = 1200  # jobs per minute
    ids_batch_size = 10

    # define_parameters() is derived from ProductsBatchParams.

    def define_source(self, runtime_params: ProductsBatchParams) -> BaseSource:
        current_ids = runtime_params.get("current_ids", [])
        return e2e_id_based_api(
            endpoint_template="/products/lookup",
            ids=current_ids,
            method="POST",
            body={"product_ids": current_ids},
            response_key="items",
            batch_size=runtime_params.get("batch_size", 5),
        )

    def define_destination(
        self, records: List[Any], runtime_params: ProductsBatchParams
    ) -> BaseDestination:
        return e2e_console(pretty_print=False, max_records_display=3)

    def define_transformations(
        self, records: List[Any], runtime_params: ProductsBatchParams
    ) -> Sequence[BaseTransformation]:
        return [
            products_tag_category(),
            products_add_tax(),
        ]
