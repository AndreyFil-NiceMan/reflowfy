"""Worker entrypoint."""

import os
import signal
import sys
from types import FrameType

from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
from reflowfy.worker.consumer import KafkaJobConsumer


def handle_shutdown(signum: int, frame: FrameType):
    """Handle graceful shutdown."""
    print("\nShutdown signal received, stopping worker...")
    sys.exit(0)


def main():
    """Worker main entry point."""
    import asyncio

    import reflowfy as reflowfy_pkg
    from reflowfy import __version__

    build_version = os.getenv("REFLOWFY_BUILD_VERSION") or os.getenv("GIT_SHA")

    print("=" * 60)
    print("🚀 Starting Reflowfy Worker (Async)")
    print(f"📦 Version: {__version__}")
    if build_version:
        print(f"🔖 Build: {build_version}")
    print(f"📂 Package path: {reflowfy_pkg.__file__}")
    print("=" * 60)

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)  # pyright: ignore[reportArgumentType]
    signal.signal(signal.SIGINT, handle_shutdown)  # pyright: ignore[reportArgumentType]

    # Auto-discover and load pipelines (registers transformations)
    pipeline_module = os.getenv("PIPELINE_MODULE", "pipelines")
    discover_and_load_pipelines(pipeline_module)
    print()

    # Get configuration from environment
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "reflow.jobs")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID", "reflowfy-workers")
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://reflowfy:reflowfy@localhost:5432/reflowfy"
    )

    # SASL Authentication config
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("KAFKA_SASL_PASSWORD")

    print(f"Kafka brokers: {kafka_bootstrap_servers}")
    print(f"Topic: {kafka_topic}")
    print(f"Consumer group: {kafka_group_id}")
    if sasl_username:
        print(
            f"SASL: {security_protocol or 'SASL_PLAINTEXT'} / {sasl_mechanism or 'SCRAM-SHA-256'}"
        )
    print(
        f"Database: {database_url.split('@')[-1] if '@' in database_url else database_url}"
    )  # Hide credentials
    print()

    # Create consumer
    consumer = KafkaJobConsumer(
        bootstrap_servers=kafka_bootstrap_servers,
        topic=kafka_topic,
        group_id=kafka_group_id,
        database_url=database_url,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )

    try:
        print("Worker ready, waiting for jobs...\n")
        # Run async consumer in event loop
        asyncio.run(consumer.start())
    except KeyboardInterrupt:
        print("\nWorker stopped by user")
    except Exception as e:
        print(f"\nWorker crashed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
