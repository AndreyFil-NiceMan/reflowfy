"""
Example pipeline demonstrating Reflowfy usage.

This shows how to:
1. Define a custom transformation
2. Configure source and destination
3. Build and register a pipeline
"""

from reflowfy import (
    build_pipeline,
    pipeline_registry,
    BaseTransformation,
    elastic_source,
    kafka_destination,
)


# 1. Define custom transformation
class XmlToJson(BaseTransformation):
    """Transform XML records to JSON."""
    
    name = "xml_to_json"
    
    def apply(self, records, context):
        """
        Parse XML and convert to JSON.
        
        Args:
            records: List of records with XML content
            context: Execution context
        
        Returns:
            Transformed records
        """
        import xml.etree.ElementTree as ET
        
        transformed = []
        
        for record in records:
            try:
                # Assume record has 'xml_data' field
                xml_str = record.get("xml_data", "")
                
                if not xml_str:
                    continue
                
                # Parse XML
                root = ET.fromstring(xml_str)
                
                # Convert to dict (simplified)
                json_record = {
                    "tag": root.tag,
                    "attributes": root.attrib,
                    "text": root.text,
                    "children": [
                        {"tag": child.tag, "text": child.text}
                        for child in root
                    ],
                }
                
                # Preserve other fields
                result = {**record, "parsed": json_record}
                transformed.append(result)
            
            except Exception as e:
                print(f"⚠️  Failed to parse XML: {e}")
                # Skip invalid records
                continue
        
        return transformed


# 2. Configure source
source = elastic_source(
    url="http://elasticsearch:9200",
    index="logs-*",
    base_query={
        "query": {
            "range": {
                "@timestamp": {
                    "gte": "{{ start_time }}",
                    "lte": "{{ end_time }}",
                }
            }
        }
    },
    scroll="2m",
    size=1000,
)

# 3. Configure destination
destination = kafka_destination(
    bootstrap_servers="kafka:9092",
    topic="processed-logs",
    compression_type="gzip",
)

# 4. Build and register pipeline
pipeline = build_pipeline(
    name="elastic_xml_pipeline",
    source=source,
    transformations=[XmlToJson()],
    destination=destination,
    rate_limit=50,
)

pipeline_registry.register(pipeline)

# That's it! The API will automatically create routes for this pipeline:
# - POST /pipelines/elastic_xml_pipeline/run
# - POST /pipelines/elastic_xml_pipeline/test
# - GET /pipelines/elastic_xml_pipeline/status
