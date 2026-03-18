"""E2E test: trace context propagation through gateway → NATS → handler."""

from __future__ import annotations

from unittest.mock import AsyncMock

import orjson
import pytest
import structlog
from opentelemetry import context, trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider

from spark_advisor_models.logging import bind_nats_context
from spark_advisor_models.tracing import (
    TRACE_ID_HEADER,
    TracingConfig,
    extract_trace_context,
    inject_correlation_context,
)


def _fresh_provider() -> TracerProvider:
    return TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))


class TestTraceContextPropagationE2E:
    """Simulates the full trace propagation flow:
    gateway injects headers → NATS message → handler extracts context.
    """

    def setup_method(self) -> None:
        trace.set_tracer_provider(_fresh_provider())
        TracingConfig._service_name = "test"
        structlog.contextvars.clear_contextvars()

    def test_otel_trace_id_propagates_through_nats_headers(self) -> None:
        tracer = trace.get_tracer("gateway-test")

        with tracer.start_as_current_span("gateway.execute_analysis") as gateway_span:
            gateway_trace_id = gateway_span.get_span_context().trace_id

            headers: dict[str, str] = {}
            inject_correlation_context(headers)
            assert "traceparent" in headers

            nats_headers = dict(headers)

        context.attach(context.Context())

        extracted_ctx = extract_trace_context(nats_headers)
        context.attach(extracted_ctx)

        handler_span_ctx = trace.get_current_span().get_span_context()
        assert handler_span_ctx.trace_id == gateway_trace_id

    def test_fallback_uuid_propagates_when_otel_disabled(self) -> None:
        context.attach(context.Context())

        headers: dict[str, str] = {}
        inject_correlation_context(headers)
        assert TRACE_ID_HEADER in headers
        original_uuid = headers[TRACE_ID_HEADER]

        bind_nats_context(headers, service="analyzer", app_id="app-1")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["trace_id"] == original_uuid

    def test_consistent_uuid_across_multiple_inject_calls(self) -> None:
        context.attach(context.Context())

        base_headers = {TRACE_ID_HEADER: "shared-uuid-123"}
        headers1 = inject_correlation_context(dict(base_headers))
        headers2 = inject_correlation_context(dict(base_headers))

        assert headers1[TRACE_ID_HEADER] == "shared-uuid-123"
        assert headers2[TRACE_ID_HEADER] == "shared-uuid-123"

    @pytest.mark.asyncio
    async def test_full_gateway_to_handler_flow_with_nats_mock(self) -> None:
        """Simulates gateway executor sending NATS request and handler receiving it."""
        tracer = trace.get_tracer("gateway")

        captured_headers: dict[str, str] = {}

        async def mock_nats_request(
            subject: str, data: bytes, timeout: float, headers: dict[str, str] | None = None,
        ) -> AsyncMock:
            if headers:
                captured_headers.update(headers)
            reply = AsyncMock()
            reply.data = orjson.dumps({"error": "test"})
            return reply

        with tracer.start_as_current_span("gateway.execute") as span:
            gateway_trace_id = span.get_span_context().trace_id
            headers = inject_correlation_context({})
            await mock_nats_request("analysis.run", b"{}", timeout=30.0, headers=headers)

        assert "traceparent" in captured_headers

        handler_ctx = extract_trace_context(captured_headers)
        context.attach(handler_ctx)
        handler_span = trace.get_current_span()
        assert handler_span.get_span_context().trace_id == gateway_trace_id
