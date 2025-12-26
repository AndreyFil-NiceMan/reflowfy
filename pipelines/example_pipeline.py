"""
Example pipeline - Copy this template to create your own pipelines.
"""

from reflowfy import (
    build_pipeline,
    pipeline_registry,
    BaseTransformation,
    elastic_source,
    kafka_destination,
)


# Define your transformation
class MyTransformation(BaseTransformation):
    """Example transformation."""
    
    name = "my_transform"
    
    def apply(self, records, context):
        """Transform records."""
        # Your transformation logic here
        return records


# Configure source
source = elastic_source(
    url="http://elasticsearch:9200",
    index="my-index-*",
    base_query={
        "query": {
            "match_all": {}
        }
    },
    scroll="2m",
    size=1000,
)

# Configure destination  
destination = kafka_destination(
    bootstrap_servers="kafka:29092",
    topic="my-output-topic",
)

# Build and register pipeline
pipeline = build_pipeline(
    name="example_pipeline",
    source=source,
    transformations=[MyTransformation()],
    destination=destination,
    rate_limit={"jobs_per_second": 50},
)

pipeline_registry.register(pipeline)
