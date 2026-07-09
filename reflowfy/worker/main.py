"""Worker entrypoint."""

import logging
import os
import signal
import sys
from types import FrameType

from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
from reflowfy.observability import metrics
from reflowfy.observability.logging import setup_logging
from reflowfy.observability.tracing import init_tracing
from reflowfy.worker.consumer import KafkaJobConsumer

from prometheus_client import start_http_server

logger = logging.getLogger(__name__)


def handle_shutdown(signum: int, frame: FrameType):
    """Handle graceful shutdown."""
    logger.info("Shutdown signal received, stopping worker")
    sys.exit(0)


def main():
    """Worker main entry point."""
    import asyncio

    import reflowfy as reflowfy_pkg
    from reflowfy import __version__

    build_version = os.getenv("REFLOWFY_BUILD_VERSION") or os.getenv("GIT_SHA")

    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)  # pyright: ignore[reportArgumentType]
    signal.signal(signal.SIGINT, handle_shutdown)  # pyright: ignore[reportArgumentType]

    # Observability: structured logging, tracing, and a /metrics HTTP server.
    setup_logging(service_name="worker")
    init_tracing(service_name="worker")
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))

    logger.info(
        "Starting Reflowfy worker (version %s, build %s, package %s)",
        __version__,
        build_version or "unknown",
        reflowfy_pkg.__file__,
    )

    # Auto-discover and load pipelines (module from PIPELINE_MODULE env)
    discover_and_load_pipelines()

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

    # Database host only — never log credentials.
    db_target = database_url.split("@")[-1] if "@" in database_url else database_url
    logger.info(
        "Kafka brokers=%s topic=%s consumer_group=%s database=%s",
        kafka_bootstrap_servers,
        kafka_topic,
        kafka_group_id,
        db_target,
    )
    if sasl_username:
        logger.info(
            "SASL enabled: %s / %s",
            security_protocol or "SASL_PLAINTEXT",
            sasl_mechanism or "SCRAM-SHA-256",
        )

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
        logger.info("Worker ready, waiting for jobs")
        # This process is one active worker; Prometheus sums the gauge across
        # all scraped worker replicas.
        metrics.active_workers.set(1)
        # Run async consumer in event loop
        asyncio.run(consumer.start())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception:
        logger.exception("Worker crashed")
        sys.exit(1)
    finally:
        metrics.active_workers.set(0)


if __name__ == "__main__":
    main()
