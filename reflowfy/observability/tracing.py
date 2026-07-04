"""OpenTelemetry tracing: init, and W3C context propagation across Kafka."""

import os
from typing import Any, Dict, Optional

from opentelemetry import trace
from opentelemetry.propagate import extract, inject

_initialized = False


def init_tracing(service_name: str = "reflowfy") -> None:
    """Configure the global tracer to export OTLP to Elastic APM.

    No-op when OTEL_EXPORTER_OTLP_ENDPOINT is blank (tracing disabled).
    """
    global _initialized
    if _initialized or not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", service_name)})
    )
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    try:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        LoggingInstrumentor().instrument(set_logging_format=False)
    except Exception:
        pass
    _initialized = True


def instrument_fastapi(app: Any) -> None:
    """Auto-instrument a FastAPI app (no-op if tracing disabled or lib missing)."""
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except Exception:
        pass


def inject_trace_context(carrier: Dict[str, Any]) -> None:
    """Inject the current span's W3C traceparent into a dict carrier (job metadata)."""
    inject(carrier)


def extract_and_attach(carrier: Optional[Dict[str, Any]]) -> Optional[Any]:
    """Extract a remote context from a carrier. Returns the extracted context."""
    return extract(carrier or {})
