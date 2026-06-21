"""
Example reusable source configuration.

Use the @source decorator to create named source configurations
that can be reused across multiple pipelines.
"""

import os
from reflowfy import source, elastic_source


@source("example_elastic")
def example_elastic(**overrides):
    """
    Example Elasticsearch source configuration.

    Usage in a pipeline:
        from sources.example_source import example_elastic

        def define_source(self, runtime_params):
            return example_elastic(index="my-specific-index")
    """
    return elastic_source(
        url=os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
        index=overrides.get("index", "logs-*"),
        base_query=overrides.get("base_query", {"query": {"match_all": {}}}),
        scroll=overrides.get("scroll", "2m"),
        size=overrides.get("size", 1000),
    )
