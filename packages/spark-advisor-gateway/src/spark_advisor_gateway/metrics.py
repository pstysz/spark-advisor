from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI
    from prometheus_client import Counter, Histogram

_tasks_total: Counter | None = None
_task_duration: Histogram | None = None
_analysis_mode_total: Counter | None = None
_nats_request_duration: Histogram | None = None
_rules_violations_total: Counter | None = None


def tasks_total_inc(status: str) -> None:
    if _tasks_total is not None:
        _tasks_total.labels(status=status).inc()


def task_duration_observe(mode: str, value: float) -> None:
    if _task_duration is not None:
        _task_duration.labels(mode=mode).observe(value)


def analysis_mode_inc(mode: str) -> None:
    if _analysis_mode_total is not None:
        _analysis_mode_total.labels(mode=mode).inc()


def nats_request_observe(operation: str, duration: float) -> None:
    if _nats_request_duration is not None:
        _nats_request_duration.labels(operation=operation).observe(duration)


def rules_violations_inc(rule: str, severity: str) -> None:
    if _rules_violations_total is not None:
        _rules_violations_total.labels(rule=rule, severity=severity).inc()


def setup_metrics(app: FastAPI, *, enabled: bool = False) -> None:
    global _tasks_total, _task_duration, _analysis_mode_total, _nats_request_duration, _rules_violations_total

    if not enabled:
        return

    from prometheus_fastapi_instrumentator import Instrumentator

    if _tasks_total is None:
        from prometheus_client import Counter, Histogram

        _tasks_total = Counter(
            "sa_tasks_total",
            "Total analysis tasks by status",
            ["status"],
        )
        _task_duration = Histogram(
            "sa_task_duration_seconds",
            "Task execution duration in seconds",
            ["mode"],
            buckets=(1, 5, 10, 30, 60, 120, 300),
        )
        _analysis_mode_total = Counter(
            "sa_analysis_mode_total",
            "Analyses by mode",
            ["mode"],
        )
        _nats_request_duration = Histogram(
            "sa_nats_request_duration_seconds",
            "NATS request-reply latency",
            ["operation"],
            buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
        )
        _rules_violations_total = Counter(
            "sa_rules_violations_total",
            "Rule violations detected",
            ["rule", "severity"],
        )

    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
