"""Worker entrypoint."""

import os
import signal
import sys
from reflowfy.worker.consumer import KafkaJobConsumer


def handle_shutdown(signum, frame):
    """Handle graceful shutdown."""
    print("\n🛑 Shutdown signal received, stopping worker...")
    sys.exit(0)


def main():
    """Worker main entry point."""
    print("=" * 60)
    print("⚙️  Starting Reflowfy Worker")
    print("=" * 60)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    # Get configuration from environment
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "reflowfy-workers")
    reflow_manager_url = os.getenv("REFLOW_MANAGER_URL", "http://localhost:8001")
    
    print(f"📡 Kafka brokers: {kafka_bootstrap_servers}")
    print(f"📥 Topic: {kafka_topic}")
    print(f"👥 Consumer group: {kafka_group_id}")
    print(f"📊 ReflowManager: {reflow_manager_url}")
    print()
    
    # Create and start consumer
    consumer = KafkaJobConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        topic=kafka_topic,
        group_id=kafka_group_id,
        reflow_manager_url=reflow_manager_url,
    )
    
    try:
        print("🚀 Worker ready, waiting for jobs...\n")
        consumer.start()
    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    except Exception as e:
        print(f"\n❌ Worker crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
