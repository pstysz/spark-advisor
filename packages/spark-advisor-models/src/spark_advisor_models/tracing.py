from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Tracer

_tracer: Tracer | None = None


def configure_tracing(service_name: str, endpoint: str, *, enabled: bool = False) -> None:
    global _tracer

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
    _tracer = trace.get_tracer(service_name)


def get_tracer() -> Tracer:
    if _tracer is not None:
        return _tracer
    from opentelemetry import trace

    return trace.get_tracer("spark-advisor-noop")


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
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    except Exception:
        pass
    return event_dict


TRACE_ID_HEADER = "X-Trace-ID"


def inject_correlation_context(headers: dict[str, str]) -> dict[str, str]:
    headers = inject_trace_context(headers)
    if "traceparent" not in headers:
        import uuid

        headers[TRACE_ID_HEADER] = str(uuid.uuid4())
    return headers


def extract_fallback_trace_id(headers: dict[str, str] | None) -> str:
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span.get_span_context().is_valid:
            return ""
    except Exception:
        pass
    return (headers or {}).get(TRACE_ID_HEADER, "")


def _build_trace_id_vars() -> dict[str, str]:
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        if span.get_span_context().is_valid:
            return {}
    except Exception:
        pass
    import uuid

    return {"trace_id": str(uuid.uuid4())}
