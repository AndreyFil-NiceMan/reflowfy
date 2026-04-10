"""
Start API for distributed testing with Kafka.

This API will:
1. Load the simple test pipeline
2. Expose /run endpoint (distributed mode)
3. Send jobs to Kafka when /run is called
"""

import os

# Set Kafka configuration for API
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9093"
os.environ["KAFKA_TOPIC"] = "reflow.jobs"

# Import pipeline definitions - this triggers auto-registration

# Start the API
from reflowfy.api.app import main

if __name__ == "__main__":
    main()
