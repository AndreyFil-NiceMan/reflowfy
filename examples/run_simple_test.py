"""
Example startup script for testing Reflowfy locally.

This script imports a simple test pipeline that requires NO external dependencies:
- No Elasticsearch
- No Kafka
- No databases

Just run this and test via Swagger UI!
"""

# Import the simple test pipeline (triggers registration)
import simple_test_pipeline

# Start the API
from reflowfy.api.app import main

if __name__ == "__main__":
    main()
