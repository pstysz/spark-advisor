from __future__ import annotations

import logging

import pytest
import structlog

from spark_advisor_models.logging import bind_nats_context, configure_logging, nats_handler_context


class TestConfigureLogging:
    def setup_method(self) -> None:
        structlog.reset_defaults()
        structlog.contextvars.clear_contextvars()
        root = logging.getLogger()
        root.handlers.clear()

    def test_configures_root_handler(self) -> None:
        configure_logging("test-service", "INFO")
        root = logging.getLogger()
        structlog_handlers = [
            h for h in root.handlers
            if isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
        ]
        assert len(structlog_handlers) == 1
        assert root.level == logging.INFO

    def test_json_renderer_produces_valid_json(self, capsys: object) -> None:
        configure_logging("test-service", "INFO", json_output=True)
        logger = structlog.stdlib.get_logger("test")
        logger.info("hello")
        import sys

        sys.stderr.flush()

    def test_console_renderer_when_json_disabled(self) -> None:
        configure_logging("test-service", "DEBUG", json_output=False)
        root = logging.getLogger()
        formatter = root.handlers[0].formatter
        assert formatter is not None

    def test_log_level_respected(self) -> None:
        configure_logging("test-service", "WARNING")
        root = logging.getLogger()
        assert root.level == logging.WARNING

    def test_log_level_case_insensitive(self) -> None:
        configure_logging("test-service", "debug")
        root = logging.getLogger()
        assert root.level == logging.DEBUG

    def test_service_bound_to_context(self) -> None:
        configure_logging("my-service", "INFO")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["service"] == "my-service"

    def test_foreign_logs_structured(self, capsys: object) -> None:
        configure_logging("test-service", "INFO", json_output=True)
        stdlib_logger = logging.getLogger("some.third.party")
        stdlib_logger.info("foreign log")

    def test_structlog_get_logger_works(self) -> None:
        configure_logging("test-service", "INFO")
        logger = structlog.stdlib.get_logger("mymodule")
        assert logger is not None

    def test_idempotent_reconfiguration(self) -> None:
        configure_logging("svc1", "INFO")
        configure_logging("svc2", "DEBUG")
        root = logging.getLogger()
        structlog_handlers = [
            h for h in root.handlers
            if isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
        ]
        assert len(structlog_handlers) == 1
        assert root.level == logging.DEBUG

    def test_preserves_non_structlog_handlers(self) -> None:
        root = logging.getLogger()
        pre_count = len(root.handlers)
        external_handler = logging.StreamHandler()
        root.addHandler(external_handler)
        configure_logging("test-service", "INFO")
        assert external_handler in root.handlers
        structlog_handlers = [
            h for h in root.handlers
            if isinstance(h.formatter, structlog.stdlib.ProcessorFormatter)
        ]
        assert len(structlog_handlers) == 1
        assert len(root.handlers) == pre_count + 2


class TestBindNatsContext:
    def setup_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    def test_binds_service_from_kwargs(self) -> None:
        bind_nats_context(None, service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["service"] == "analyzer"

    def test_binds_extra_kwargs(self) -> None:
        bind_nats_context(None, app_id="app-42", service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["app_id"] == "app-42"
        assert ctx["service"] == "analyzer"

    def test_unbinds_transient_keys_preserving_persistent(self) -> None:
        structlog.contextvars.bind_contextvars(service="my-service", old_key="preserved")
        bind_nats_context(None, app_id="app-1")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx.get("service") == "my-service"
        assert ctx.get("old_key") == "preserved"
        assert ctx.get("app_id") == "app-1"

    def test_clears_transient_keys_from_previous_call(self) -> None:
        structlog.contextvars.bind_contextvars(service="svc")
        bind_nats_context(None, app_id="app-1", service="svc")
        bind_nats_context(None, service="svc")
        ctx = structlog.contextvars.get_contextvars()
        assert "app_id" not in ctx

    def test_attaches_otel_context_from_traceparent(self) -> None:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))
        trace.set_tracer_provider(provider)
        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("parent") as span:
            from spark_advisor_models.tracing import inject_trace_context

            headers: dict[str, str] = {}
            inject_trace_context(headers)

        bind_nats_context(headers, service="analyzer")
        current_span = trace.get_current_span()
        assert current_span.get_span_context().trace_id == span.get_span_context().trace_id

    def test_no_fallback_trace_id_when_otel_active(self) -> None:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))
        trace.set_tracer_provider(provider)
        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("parent"):
            from spark_advisor_models.tracing import inject_trace_context

            headers: dict[str, str] = {}
            inject_trace_context(headers)

        bind_nats_context(headers, service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert "trace_id" not in ctx

    def test_trace_id_fallback_to_header(self) -> None:
        from spark_advisor_models.tracing import TRACE_ID_HEADER

        bind_nats_context({TRACE_ID_HEADER: "uuid-123"}, service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["trace_id"] == "uuid-123"

    def test_no_trace_id_when_no_otel_no_header(self) -> None:
        bind_nats_context(None, service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert "trace_id" not in ctx


class TestNatsHandlerContext:
    def setup_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    @pytest.mark.asyncio
    async def test_binds_context_and_creates_span(self) -> None:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))
        trace.set_tracer_provider(provider)

        async with nats_handler_context(
            None, "test.span", {"key": "val"}, service="my-svc",
        ):
            span = trace.get_current_span()
            assert span.get_span_context().is_valid
            ctx = structlog.contextvars.get_contextvars()
            assert ctx["service"] == "my-svc"

    @pytest.mark.asyncio
    async def test_span_ends_after_context_exit(self) -> None:
        from opentelemetry import context, trace
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider

        provider = TracerProvider(resource=Resource.create({SERVICE_NAME: "test"}))
        trace.set_tracer_provider(provider)
        context.attach(context.Context())

        async with nats_handler_context(
            None, "test.span", {}, service="svc",
        ):
            pass

        span = trace.get_current_span()
        assert not span.get_span_context().is_valid
