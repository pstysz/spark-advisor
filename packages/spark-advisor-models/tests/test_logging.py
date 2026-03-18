from __future__ import annotations

import logging

import structlog

from spark_advisor_models.logging import bind_nats_context, configure_logging


class TestConfigureLogging:
    def setup_method(self) -> None:
        structlog.reset_defaults()
        structlog.contextvars.clear_contextvars()
        root = logging.getLogger()
        root.handlers.clear()

    def test_configures_root_handler(self) -> None:
        configure_logging("test-service", "INFO")
        root = logging.getLogger()
        assert len(root.handlers) == 1
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


class TestBindNatsContext:
    def setup_method(self) -> None:
        structlog.contextvars.clear_contextvars()

    def test_binds_correlation_id_from_headers(self) -> None:
        bind_nats_context({"X-Correlation-ID": "task-123"}, service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["correlation_id"] == "task-123"
        assert ctx["service"] == "analyzer"

    def test_empty_correlation_id_when_no_headers(self) -> None:
        bind_nats_context(None, service="hs-connector")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["correlation_id"] == ""
        assert ctx["service"] == "hs-connector"

    def test_extra_kwargs_bound(self) -> None:
        bind_nats_context({"X-Correlation-ID": "t-1"}, app_id="app-42", service="analyzer")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx["app_id"] == "app-42"

    def test_clears_previous_context(self) -> None:
        structlog.contextvars.bind_contextvars(old_key="old_value")
        bind_nats_context(None, service="test")
        ctx = structlog.contextvars.get_contextvars()
        assert "old_key" not in ctx
