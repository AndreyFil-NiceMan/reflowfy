"""Worker entrypoint."""

import logging
import os
import signal
import sys
from types import FrameType
from typing import Optional

from reflowfy.store.db_settings import database_url as env_database_url
from reflowfy.core.kafka_settings import KafkaSettings
from reflowfy.core.pipeline_discovery import discover_and_load_pipelines
from reflowfy.observability import metrics
from reflowfy.observability.logging import setup_logging
from reflowfy.observability.tracing import init_tracing
from reflowfy.worker.consumer import KafkaJobConsumer

from prometheus_client import start_http_server
from reflowfy.store.leases import JobLeases

# Named explicitly so `python -m reflowfy.worker.main` logs through the "reflowfy" logger too.
logger = logging.getLogger("reflowfy.worker.main")


def main():
    """Worker main entry point."""
    import asyncio

    import reflowfy as reflowfy_pkg
    from reflowfy import __version__

    build_version = os.getenv("REFLOWFY_BUILD_VERSION") or os.getenv("GIT_SHA")

    # Observability: structured logging, tracing, and a /metrics HTTP server.
    setup_logging(service_name="worker")
    init_tracing(service_name="worker")
    start_http_server(int(os.getenv("METRICS_PORT", "9100")))

    logger.info(
        "Starting Reflowfy worker (version %s, build %s, package %s)",
        __version__,
        build_version or "unknown",
        reflowfy_pkg.__file__,
        extra={"event": "service_start", "build_version": build_version or "unknown"},
    )

    # Auto-discover and load pipelines (module from PIPELINE_MODULE env)
    discover_and_load_pipelines()

    kafka = KafkaSettings.from_env()
    JobLeases.from_env().warn_if_inconsistent(logger)
    database_url = env_database_url()

    # Database host only — never log credentials.
    db_target = database_url.split("@")[-1] if "@" in database_url else database_url
    logger.info(
        "Kafka brokers=%s topic=%s consumer_group=%s database=%s",
        kafka.bootstrap_servers,
        kafka.topic,
        kafka.group_id,
        db_target,
        extra={
            "event": "service_config",
            "kafka_topic": kafka.topic,
            "kafka_group_id": kafka.group_id,
        },
    )
    if kafka.sasl_username:
        logger.info("SASL enabled: %s", kafka.describe_auth(), extra={"event": "service_config"})

    # Create consumer
    consumer = KafkaJobConsumer(
        bootstrap_servers=kafka.bootstrap_servers,
        topic=kafka.topic,
        group_id=kafka.group_id,
        database_url=database_url,
        security_protocol=kafka.security_protocol,
        sasl_mechanism=kafka.sasl_mechanism,
        sasl_username=kafka.sasl_username,
        sasl_password=kafka.sasl_password,
    )

    def handle_shutdown(signum: int, frame: Optional[FrameType]) -> None:
        """Drain: stop taking messages, let the current job finish and commit, exit.

        Exiting mid-job (the old sys.exit here) left a half-written job for the
        lease to re-run, duplicating what was already sent. A second signal exits
        at once. A job longer than the pod's terminationGracePeriodSeconds is still
        SIGKILLed; the manager's running lease re-runs it.
        """
        if consumer.stop_requested:
            logger.warning(
                "Second shutdown signal, exiting now", extra={"event": "shutdown_forced"}
            )
            sys.exit(1)
        logger.info(
            "Shutdown signal received; finishing the current job, then stopping",
            extra={"event": "shutdown_drain"},
        )
        consumer.request_stop()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    try:
        logger.info("Worker ready, waiting for jobs", extra={"event": "worker_ready"})
        # This process is one active worker; Prometheus sums the gauge across
        # all scraped worker replicas.
        metrics.active_workers.set(1)
        # Run async consumer in event loop
        asyncio.run(consumer.start())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user", extra={"event": "service_stop"})
    except Exception:
        logger.exception("Worker crashed", extra={"event": "worker_crashed"})
        sys.exit(1)
    finally:
        metrics.active_workers.set(0)


if __name__ == "__main__":
    main()
