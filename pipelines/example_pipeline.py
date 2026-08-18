"""
Example pipeline - Copy this template to create your own pipelines.
"""

from reflowfy import (
    Transformations,
    BaseDestination,
    Records,
    AbstractPipeline,
    RuntimeParams,
    BaseTransformation,
    elastic_source,
    kafka_destination,
)


# Define your transformation
class MyTransformation(BaseTransformation):
    """Example transformation."""

    name = "my_transform"

    def apply(self, records, runtime_params):
        """Transform records."""
        # Your transformation logic here
        return records


class ExamplePipeline(AbstractPipeline[RuntimeParams]):
    """
    Example pipeline template.

    Copy this file and customize for your use case.
    """

    name = "example_pipeline"
    rate_limit = 50

    # Declare parameters on ExampleParams above; define_parameters() is derived.

    def define_source(self, runtime_params):
        """Configure your source."""
        return elastic_source(
            url="http://elasticsearch:9200",
            index="my-index-*",
            base_query={"query": {"match_all": {}}},
            scroll="2m",
            size=1000,
        )

    def define_destination(
        self, records: Records, runtime_params: RuntimeParams
    ) -> BaseDestination:
        """Configure your destination."""
        return kafka_destination(
            bootstrap_servers="kafka:29092",
            topic="my-output-topic",
        )

    def define_transformations(
        self, records: Records, runtime_params: RuntimeParams
    ) -> Transformations:
        """Define your transformation pipeline."""
        return [MyTransformation()]
