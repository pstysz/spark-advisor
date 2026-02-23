from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry import metrics, trace

if TYPE_CHECKING:
    from spark_advisor_shared.config.base import BaseServiceSettings


def init_telemetry(service_name: str, settings: BaseServiceSettings) -> None:
    if not settings.otel_enabled:
        return

    # TODO: configure TracerProvider + MeterProvider with OTLP exporter
    # For now this is a no-op stub — OTel SDK defaults to NoOp providers
    _ = service_name
    _ = settings.otel_exporter_otlp_endpoint


def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


def get_meter(name: str) -> metrics.Meter:
    return metrics.get_meter(name)
