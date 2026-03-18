from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI
    from prometheus_client import Counter, Histogram

_tasks_total: Counter | None = None
_task_duration: Histogram | None = None
_metrics_initialized = False


def tasks_total_inc(status: str) -> None:
    if _tasks_total is not None:
        _tasks_total.labels(status=status).inc()


def task_duration_observe(mode: str, value: float) -> None:
    if _task_duration is not None:
        _task_duration.labels(mode=mode).observe(value)


def setup_metrics(app: FastAPI, *, enabled: bool = False) -> None:
    global _tasks_total, _task_duration, _metrics_initialized

    if not enabled:
        return

    from prometheus_fastapi_instrumentator import Instrumentator

    if not _metrics_initialized:
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
        _metrics_initialized = True

    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)
