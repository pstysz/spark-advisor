from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import structlog

from spark_advisor_models.tracing import add_otel_trace_context, get_tracer

__all__ = [
    "bind_nats_context",
    "configure_logging",
    "nats_handler_context",
]

_TRANSIENT_CONTEXT_KEYS = ("trace_id", "app_id", "task_id")


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
    root.handlers = [
        h for h in root.handlers
        if not isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
    ]
    root.addHandler(handler)
    root.setLevel(log_level.upper())

    structlog.contextvars.bind_contextvars(service=service)


def bind_nats_context(headers: dict[str, str] | None, **extra: str) -> None:
    from opentelemetry import context

    from spark_advisor_models.tracing import extract_fallback_trace_id, extract_trace_context

    otel_ctx = extract_trace_context(headers)
    context.attach(otel_ctx)

    fallback_trace_id = extract_fallback_trace_id(headers)

    structlog.contextvars.unbind_contextvars(*_TRANSIENT_CONTEXT_KEYS)

    if fallback_trace_id:
        structlog.contextvars.bind_contextvars(trace_id=fallback_trace_id, **extra)
    else:
        structlog.contextvars.bind_contextvars(**extra)


@asynccontextmanager
async def nats_handler_context(
    msg_headers: dict[str, str] | None,
    span_name: str,
    span_attributes: dict[str, Any],
    **log_context: str,
) -> AsyncIterator[None]:
    bind_nats_context(msg_headers, **log_context)
    with get_tracer().start_as_current_span(span_name, attributes=span_attributes):
        yield
