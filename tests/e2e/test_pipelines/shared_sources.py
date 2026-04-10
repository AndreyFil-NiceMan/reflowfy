"""
Shared reusable source definitions for E2E tests.

Demonstrates the @source decorator pattern — these sources can be
imported and reused across multiple test pipelines.

All sources use explicit keyword arguments for full IDE autocomplete.
"""

import os
from typing import List, Optional, Union

from reflowfy import source
from reflowfy.sources.mock import mock_source, generate_sample_data


# ============================================================================
# Reusable Source Configurations
# ============================================================================

@source("e2e_elastic")
def e2e_elastic(
    url: str = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201"),
    index: str = "e2e-test-events",
    scroll: str = "2m",
    size: int = 50,
    base_query: Optional[dict] = None,
):
    """Pre-configured Elasticsearch source for E2E tests."""
    from reflowfy import elastic_source

    return elastic_source(
        url=url,
        index=index,
        base_query=base_query or {"query": {"match_all": {}}},
        scroll=scroll,
        size=size,
    )


@source("e2e_mock")
def e2e_mock(
    count: int = 100,
    batch_size: int = 10,
    data: Optional[list] = None,
):
    """Pre-configured mock data source for E2E tests."""
    return mock_source(
        data=data or generate_sample_data(count=count),
        batch_size=batch_size,
    )


@source("e2e_paginated_api")
def e2e_paginated_api(
    base_url: str = os.getenv("MOCK_API_URL", "http://localhost:8092"),
    endpoint: str = "/users",
    pagination_type: str = "offset",
    page_size: int = 10,
    data_key: str = "data",
    total_key: str = "total",
    offset_param: str = "offset",
    limit_param: str = "limit",
):
    """Pre-configured paginated API source for E2E tests."""
    from reflowfy.sources.api import paginated_api_source

    return paginated_api_source(
        base_url=base_url,
        endpoint=endpoint,
        pagination_type=pagination_type,
        page_size=page_size,
        data_key=data_key,
        total_key=total_key,
        offset_param=offset_param,
        limit_param=limit_param,
    )


@source("e2e_id_based_api")
def e2e_id_based_api(
    base_url: str = os.getenv("MOCK_API_URL", "http://localhost:8092"),
    endpoint_template: str = "/users/{id}",
    ids: Optional[List[Union[str, int]]] = None,
    method: str = "GET",
    batch_size: int = 2,
    batch_id_key: Optional[str] = "ids",
    data_key: Optional[str] = None,
    request_body: Optional[dict] = None,
    query_params: Optional[dict] = None,
):
    """
    Pre-configured ID-based API source for E2E tests.

    Mode is auto-detected from endpoint_template:
    - ``{id}`` present → per-ID (one request per ID)
    - No ``{id}``      → batch (one request, IDs in body)

    Body shape (batch mode):
    - ``batch_id_key="ids"`` → ``{"ids": [...]}``
    - ``batch_id_key=None``  → ``[...]``  raw list
    """
    from reflowfy.sources.api import id_based_api_source

    return id_based_api_source(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids or [1, 2, 3, 4, 5],
        method=method,
        batch_size=batch_size,
        batch_id_key=batch_id_key,
        data_key=data_key,
        request_body=request_body,
        query_params=query_params,
    )


@source("e2e_sql")
def e2e_sql(
    query: str = "",
    connection_url: str = os.getenv(
        "SQL_CONNECTION_URL",
        "postgresql://reflowfy:reflowfy@localhost:5433/reflowfy",
    ),
    id_column: str = "id",
    batch_size: int = 50,
):
    """Pre-configured SQL source for E2E tests."""
    from reflowfy import sql_source

    return sql_source(
        connection_url=connection_url,
        query=query,
        id_column=id_column,
        batch_size=batch_size,
    )
