from __future__ import annotations

from opentelemetry import context, trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider

from spark_advisor_models.tracing import (
    TRACE_ID_HEADER,
    _build_trace_id_vars,
    add_otel_trace_context,
    configure_tracing,
    extract_fallback_trace_id,
    extract_trace_context,
    get_tracer,
    inject_correlation_context,
    inject_trace_context,
)


def _fresh_provider() -> TracerProvider:
    return TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))


class TestConfigureTracing:
    def setup_method(self) -> None:
        import spark_advisor_models.tracing as mod

        mod._tracer = None
        trace.set_tracer_provider(_fresh_provider())

    def test_disabled_does_not_set_tracer(self) -> None:
        configure_tracing("test", "http://localhost:4317", enabled=False)
        import spark_advisor_models.tracing as mod

        assert mod._tracer is None

    def test_get_tracer_returns_noop_when_not_configured(self) -> None:
        tracer = get_tracer()
        assert tracer is not None


class TestTraceContextPropagation:
    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())

    def test_inject_extract_roundtrip(self) -> None:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent") as span:
            headers: dict[str, str] = {}
            inject_trace_context(headers)
            assert "traceparent" in headers

            extracted_ctx = extract_trace_context(headers)
            extracted_span = trace.get_current_span(extracted_ctx)
            assert extracted_span.get_span_context().trace_id == span.get_span_context().trace_id

    def test_extract_none_headers_returns_context(self) -> None:
        ctx = extract_trace_context(None)
        assert ctx is not None

    def test_extract_empty_headers_returns_context(self) -> None:
        ctx = extract_trace_context({})
        assert ctx is not None

    def test_inject_without_active_span(self) -> None:
        context.attach(context.Context())
        headers: dict[str, str] = {}
        result = inject_trace_context(headers)
        assert result is headers


class TestOtelTraceContextProcessor:
    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())

    def test_adds_trace_id_and_span_id_with_active_span(self) -> None:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("test-span") as span:
            event_dict: dict[str, object] = {"event": "hello"}
            result = add_otel_trace_context(None, "info", event_dict)
            assert "trace_id" in result
            assert "span_id" in result
            assert result["trace_id"] == format(span.get_span_context().trace_id, "032x")

    def test_no_trace_fields_without_active_span(self) -> None:
        context.attach(context.Context())
        event_dict: dict[str, object] = {"event": "hello"}
        result = add_otel_trace_context(None, "info", event_dict)
        assert "trace_id" not in result
        assert "span_id" not in result


class TestInjectCorrelationContext:
    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())

    def test_with_otel_injects_traceparent_not_fallback(self) -> None:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent"):
            headers: dict[str, str] = {}
            result = inject_correlation_context(headers)
            assert "traceparent" in result
            assert TRACE_ID_HEADER not in result

    def test_without_otel_injects_fallback_uuid(self) -> None:
        context.attach(context.Context())
        headers: dict[str, str] = {}
        result = inject_correlation_context(headers)
        assert "traceparent" not in result
        assert TRACE_ID_HEADER in result
        import uuid

        uuid.UUID(result[TRACE_ID_HEADER])


class TestExtractFallbackTraceId:
    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())

    def test_returns_empty_when_otel_active(self) -> None:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent"):
            headers: dict[str, str] = {}
            inject_trace_context(headers)

        extracted_ctx = extract_trace_context(headers)
        context.attach(extracted_ctx)
        assert extract_fallback_trace_id(headers) == ""

    def test_returns_header_value_without_otel(self) -> None:
        context.attach(context.Context())
        headers = {TRACE_ID_HEADER: "my-uuid-123"}
        assert extract_fallback_trace_id(headers) == "my-uuid-123"

    def test_returns_empty_when_no_header(self) -> None:
        context.attach(context.Context())
        assert extract_fallback_trace_id(None) == ""
        assert extract_fallback_trace_id({}) == ""


class TestBuildTraceIdVars:
    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())

    def test_empty_with_active_otel_span(self) -> None:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("parent"):
            assert _build_trace_id_vars() == {}

    def test_uuid_without_otel_span(self) -> None:
        context.attach(context.Context())
        result = _build_trace_id_vars()
        assert "trace_id" in result
        import uuid

        uuid.UUID(result["trace_id"])
