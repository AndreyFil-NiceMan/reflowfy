"""init_tracing / instrument_fastapi: on with an OTLP endpoint, inert without one.

The OTel globals (tracer provider, logging instrumentation) are process-wide and
set-once, so every test stubs the pieces init_tracing would install rather than
installing them for real.
"""

from typing import Any, Dict, List

import pytest
from fastapi import FastAPI

import opentelemetry.exporter.otlp.proto.http.trace_exporter as otlp_exporter
import opentelemetry.instrumentation.fastapi as otel_fastapi
import opentelemetry.instrumentation.logging as otel_logging
import opentelemetry.sdk.trace.export as sdk_export
from reflowfy.observability import tracing


class _FakeExporter:
    def __init__(self) -> None:
        self.created = True


class _FakeProcessor:
    def __init__(self, exporter: Any) -> None:
        self.exporter = exporter

    def on_start(self, *args: Any, **kwargs: Any) -> None:
        pass

    def on_end(self, *args: Any, **kwargs: Any) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


@pytest.fixture
def otel(monkeypatch) -> Dict[str, List[Any]]:
    """Stub exporter, processor, provider install and logging instrumentation."""
    seen: Dict[str, List[Any]] = {"providers": [], "logging": []}

    monkeypatch.setattr(tracing, "_initialized", False)
    monkeypatch.setattr(otlp_exporter, "OTLPSpanExporter", _FakeExporter)
    monkeypatch.setattr(sdk_export, "BatchSpanProcessor", _FakeProcessor)
    monkeypatch.setattr(tracing.trace, "set_tracer_provider", seen["providers"].append)

    class _FakeLoggingInstrumentor:
        def instrument(self, **kwargs: Any) -> None:
            seen["logging"].append(kwargs)

    monkeypatch.setattr(otel_logging, "LoggingInstrumentor", _FakeLoggingInstrumentor)
    return seen


class TestInitTracing:
    def test_disabled_without_endpoint(self, monkeypatch, otel):
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        tracing.init_tracing("api")
        assert otel["providers"] == []
        assert otel["logging"] == []
        assert tracing._initialized is False

    def test_blank_endpoint_counts_as_disabled(self, monkeypatch, otel):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        tracing.init_tracing("api")
        assert otel["providers"] == []

    def test_enabled_installs_provider_named_after_service(self, monkeypatch, otel):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")
        monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)

        tracing.init_tracing("worker")

        assert len(otel["providers"]) == 1
        provider = otel["providers"][0]
        assert provider.resource.attributes["service.name"] == "worker"
        processors = provider._active_span_processor._span_processors
        assert [type(p) for p in processors] == [_FakeProcessor]
        assert isinstance(processors[0].exporter, _FakeExporter)
        assert otel["logging"] == [{"set_logging_format": False}]
        assert tracing._initialized is True

    def test_otel_service_name_env_wins(self, monkeypatch, otel):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")
        monkeypatch.setenv("OTEL_SERVICE_NAME", "custom-name")
        tracing.init_tracing("api")
        assert otel["providers"][0].resource.attributes["service.name"] == "custom-name"

    def test_second_call_is_a_no_op(self, monkeypatch, otel):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")
        tracing.init_tracing("api")
        tracing.init_tracing("api")
        assert len(otel["providers"]) == 1

    def test_logging_instrumentation_failure_does_not_break_init(self, monkeypatch, otel):
        class _Broken:
            def instrument(self, **kwargs: Any) -> None:
                raise RuntimeError("already instrumented")

        monkeypatch.setattr(otel_logging, "LoggingInstrumentor", _Broken)
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")

        tracing.init_tracing("api")

        assert len(otel["providers"]) == 1
        assert tracing._initialized is True


class TestInstrumentFastapi:
    @pytest.fixture
    def instrumented(self, monkeypatch) -> List[Any]:
        apps: List[Any] = []
        monkeypatch.setattr(
            otel_fastapi.FastAPIInstrumentor, "instrument_app", staticmethod(apps.append)
        )
        return apps

    def test_no_op_without_endpoint(self, monkeypatch, instrumented):
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        tracing.instrument_fastapi(FastAPI())
        assert instrumented == []

    def test_instruments_app_when_enabled(self, monkeypatch, instrumented):
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")
        app = FastAPI()
        tracing.instrument_fastapi(app)
        assert instrumented == [app]

    def test_instrumentation_error_is_swallowed(self, monkeypatch):
        def boom(app: Any) -> None:
            raise RuntimeError("instrumentation unavailable")

        monkeypatch.setattr(otel_fastapi.FastAPIInstrumentor, "instrument_app", staticmethod(boom))
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://apm:4318")
        tracing.instrument_fastapi(FastAPI())  # must not raise
