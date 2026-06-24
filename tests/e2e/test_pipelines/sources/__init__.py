"""
Reusable source configurations for E2E test pipelines.

All sources use the @source decorator and are importable as factory functions.
"""

import os
from typing import List, Optional, Union

from reflowfy import source
from reflowfy.sources.mock import mock_source, generate_sample_data


@source("e2e_elastic")
def e2e_elastic(
    url: str = os.getenv("ELASTICSEARCH_URL", "http://localhost:9201"),
    index: str = "e2e-test-events",
    scroll: str = "2m",
    size: int = 50,
    base_query: Optional[dict] = None,
    num_slices: int = 1,
):
    """Pre-configured Elasticsearch source for E2E tests.

    ``num_slices`` (default 1, i.e. no slicing -> a single job) is forwarded
    to ``elastic_source``/``ElasticSource.split()``. Pass > 1 only where a
    test specifically needs multiple parallel sliced-scroll jobs (v2
    worker-side sourcing produces one job per slice, not per scroll page).
    """
    from reflowfy import elastic_source

    return elastic_source(
        url=url,
        index=index,
        base_query=base_query or {"query": {"match_all": {}}},
        scroll=scroll,
        size=size,
        num_slices=num_slices,
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


@source("e2e_id_based_api")
def e2e_id_based_api(
    base_url: str = os.getenv("MOCK_API_URL", "http://localhost:8092"),
    endpoint_template: str = "/users/{id}",
    ids: Optional[List[Union[str, int]]] = None,
    method: str = "GET",
    batch_size: int = 2,
    response_key: Optional[str] = None,
    body: Optional[object] = None,
    params: Optional[dict] = None,
):
    """Pre-configured ID-based API source for E2E tests."""
    from reflowfy.sources.api import id_based_api_source

    return id_based_api_source(
        base_url=base_url,
        endpoint_template=endpoint_template,
        ids=ids or [1, 2, 3, 4, 5],
        method=method,
        batch_size=batch_size,
        response_key=response_key,
        body=body,
        params=params,
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
