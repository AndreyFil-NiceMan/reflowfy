"""
Example pipeline - Copy this template to create your own pipelines.
"""

from reflowfy import (
    AbstractPipeline,
    PipelineParameter,
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


class ExamplePipeline(AbstractPipeline):
    """
    Example pipeline template.
    
    Copy this file and customize for your use case.
    """
    
    name = "example_pipeline"
    rate_limit = {"jobs_per_second": 50}
    
    def define_parameters(self):
        """Define your runtime parameters here."""
        return [
            # Add parameters as needed:
            # PipelineParameter(name="param_name", required=True, description="..."),
        ]
    
    def define_source(self, params):
        """Configure your source."""
        return elastic_source(
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
    
    def define_destination(self, params):
        """Configure your destination."""
        return kafka_destination(
            bootstrap_servers="kafka:29092",
            topic="my-output-topic",
        )
    
    def define_transformations(self, params):
        """Define your transformation pipeline."""
        return [MyTransformation()]

