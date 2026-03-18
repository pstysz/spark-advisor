from __future__ import annotations

import logging

import structlog

from spark_advisor_models.tracing import add_otel_trace_context


def configure_logging(service: str, log_level: str, *, json_output: bool = True) -> None:
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        add_otel_trace_context,  # type: ignore[list-item]
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer = structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(log_level.upper())

    structlog.contextvars.bind_contextvars(service=service)


def bind_nats_context(headers: dict[str, str] | None, **extra: str) -> None:
    from opentelemetry import context

    from spark_advisor_models.tracing import extract_fallback_trace_id, extract_trace_context

    otel_ctx = extract_trace_context(headers)
    context.attach(otel_ctx)

    fallback_trace_id = extract_fallback_trace_id(headers)

    structlog.contextvars.clear_contextvars()
    if fallback_trace_id:
        structlog.contextvars.bind_contextvars(trace_id=fallback_trace_id, **extra)
    else:
        structlog.contextvars.bind_contextvars(**extra)
