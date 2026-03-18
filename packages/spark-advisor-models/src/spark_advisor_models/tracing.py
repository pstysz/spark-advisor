from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Tracer

TRACE_ID_HEADER = "X-Trace-ID"

__all__ = [
    "TRACE_ID_HEADER",
    "TracingConfig",
    "add_otel_trace_context",
    "build_trace_context_vars",
    "configure_tracing",
    "extract_fallback_trace_id",
    "extract_trace_context",
    "get_tracer",
    "inject_correlation_context",
    "inject_trace_context",
]


class TracingConfig:
    _service_name: str = "spark-advisor"

    @classmethod
    def configure(cls, service_name: str, endpoint: str, *, enabled: bool = False) -> None:
        cls._service_name = service_name

        if not enabled:
            return

        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create({SERVICE_NAME: service_name})
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        trace.set_tracer_provider(provider)

    @classmethod
    def get_tracer(cls) -> Tracer:
        from opentelemetry import trace

        return trace.get_tracer(cls._service_name)


def configure_tracing(service_name: str, endpoint: str, *, enabled: bool = False) -> None:
    TracingConfig.configure(service_name, endpoint, enabled=enabled)


def get_tracer() -> Tracer:
    return TracingConfig.get_tracer()


def inject_trace_context(headers: dict[str, str]) -> dict[str, str]:
    from opentelemetry import context
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    TraceContextTextMapPropagator().inject(headers, context=context.get_current())
    return headers


def extract_trace_context(headers: dict[str, str] | None) -> Context:
    from opentelemetry import context
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    if not headers:
        return context.get_current()
    return TraceContextTextMapPropagator().extract(carrier=headers)


def add_otel_trace_context(
    logger: Any, method: str, event_dict: dict[str, Any],
) -> dict[str, Any]:
    from opentelemetry import trace

    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx.is_valid:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def inject_correlation_context(headers: dict[str, str]) -> dict[str, str]:
    headers = inject_trace_context(headers)
    if "traceparent" not in headers and TRACE_ID_HEADER not in headers:
        headers[TRACE_ID_HEADER] = str(uuid.uuid4())
    return headers


def extract_fallback_trace_id(headers: dict[str, str] | None) -> str:
    from opentelemetry import trace

    span = trace.get_current_span()
    if span.get_span_context().is_valid:
        return ""
    return (headers or {}).get(TRACE_ID_HEADER, "")


def build_trace_context_vars() -> dict[str, str]:
    from opentelemetry import trace

    span = trace.get_current_span()
    if span.get_span_context().is_valid:
        return {}
    return {"trace_id": str(uuid.uuid4())}
