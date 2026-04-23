"""
Reusable destination configurations for E2E test pipelines.

All destinations use the @destination decorator and are importable as factory functions.
"""

import os
from typing import Dict, Optional

from reflowfy import destination
from reflowfy.destinations.console import console_destination
from reflowfy.destinations.http import http_destination


@destination("e2e_http")
def e2e_http(
    url: str = os.getenv("MOCK_HTTP_URL", "http://localhost:8091/webhook"),
    method: str = "POST",
    headers: Optional[Dict[str, str]] = None,
    auth_type: str = "bearer",
    auth_token: str = "test-webhook-token",
    batch_requests: bool = True,
    timeout: float = 30.0,
):
    """Pre-configured HTTP webhook destination for E2E tests."""
    return http_destination(
        url=url,
        method=method,
        headers=headers or {"Content-Type": "application/json"},
        auth_type=auth_type,
        auth_token=auth_token,
        batch_requests=batch_requests,
        timeout=timeout,
    )


@destination("e2e_console")
def e2e_console(
    pretty_print: bool = True,
    max_records_display: int = 5,
):
    """Pre-configured console destination for E2E tests."""
    return console_destination(
        pretty_print=pretty_print,
        max_records_display=max_records_display,
    )


@destination("e2e_kafka")
def e2e_kafka(
    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.getenv("E2E_KAFKA_SERVERS", "localhost:9095"),
    ),
    topic: str = os.getenv("E2E_KAFKA_DEST_TOPIC", "e2e-test-destination"),
    compression_type: str = "gzip",
    batch_size: int = 16384,
    linger_ms: int = 10,
):
    """Pre-configured Kafka destination for E2E tests."""
    from reflowfy.destinations.kafka import kafka_destination

    return kafka_destination(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        compression_type=compression_type,
        batch_size=batch_size,
        linger_ms=linger_ms,
    )
