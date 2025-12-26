"""
Start Reflowfy worker locally to consume jobs from Kafka.

This worker will:
1. Connect to local Kafka (localhost:9092)
2. Consume from the reflow.jobs topic
3. Execute transformations
4. Send results to destinations
"""

import sys
import os

# Add project root to path so we can import from pipelines/
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Make sure transformations are registered
from pipelines import simple_test_pipeline
from pipelines import elastic_test_pipeline  # Add elastic transformations

# Now start the worker
from reflowfy.worker.main import main

if __name__ == "__main__":
    # Set environment variables for local Kafka
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9093"
    os.environ["KAFKA_TOPIC"] = "reflow.jobs"
    os.environ["KAFKA_GROUP_ID"] = "reflowfy-workers-local"
    
    print("=" * 60)
    print("🚀 Starting Local Reflowfy Worker")
    print("=" * 60)
    print("📡 Kafka: localhost:9093")
    print("📥 Topic: reflow.jobs")
    print("👥 Group: reflowfy-workers-local")
    print()
    print("Worker will consume jobs and execute transformations...")
    print("=" * 60)
    print()
    
    main()
