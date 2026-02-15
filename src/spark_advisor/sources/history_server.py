"""Data sources for fetching Spark job metrics.

Protocol = Python equivalent of Kotlin/Java interface.
Each source knows how to fetch job data and convert it to our domain model.
"""

from typing import Protocol

import httpx

from spark_advisor.models import (
    ExecutorMetrics,
    JobAnalysis,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
)


class DataSource(Protocol):
    """Interface for fetching Spark job metrics — equivalent of Kotlin interface."""

    def fetch(self, app_id: str) -> JobAnalysis: ...

    def list_applications(self, limit: int = 20) -> list[dict[str, str]]: ...


class HistoryServerSource:
    """Fetches job data from Spark History Server REST API.

    This is the preferred source — no need to manually copy event logs.
    History Server aggregates data for you.

    Usage:
        source = HistoryServerSource("http://yarn-master:18080")
        job = source.fetch("application_1234567890_0001")
    """

    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=f"{self._base_url}/api/v1",
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    def list_applications(self, limit: int = 20) -> list[dict[str, str]]:
        response = self._client.get("/applications", params={"limit": limit})
        response.raise_for_status()
        return [
            {
                "app_id": app["id"],
                "name": app.get("name", ""),
                "start": app.get("attempts", [{}])[0].get("startTime", ""),
                "duration_ms": str(app.get("attempts", [{}])[0].get("duration", 0)),
            }
            for app in response.json()
        ]

    def fetch(self, app_id: str) -> JobAnalysis:
        app_info = self._fetch_app_info(app_id)
        config = self._fetch_config(app_id)
        stages = self._fetch_stages(app_id)
        executors = self._fetch_executors(app_id)

        duration = app_info.get("attempts", [{}])[0].get("duration", 0)

        return JobAnalysis(
            app_id=app_id,
            app_name=app_info.get("name", ""),
            duration_ms=duration,
            config=config,
            stages=stages,
            executors=executors,
        )

    def _fetch_app_info(self, app_id: str) -> dict:
        response = self._client.get(f"/applications/{app_id}")
        response.raise_for_status()
        return response.json()

    def _fetch_config(self, app_id: str) -> SparkConfig:
        response = self._client.get(f"/applications/{app_id}/environment")
        response.raise_for_status()
        env = response.json()

        spark_props: dict[str, str] = {}
        for prop in env.get("sparkProperties", []):
            if len(prop) >= 2:
                spark_props[prop[0]] = prop[1]

        return SparkConfig(raw=spark_props)

    def _fetch_stages(self, app_id: str) -> list[StageMetrics]:
        response = self._client.get(
            f"/applications/{app_id}/stages",
            params={"status": "complete"},
        )
        response.raise_for_status()

        stages: list[StageMetrics] = []
        for stage_data in response.json():
            task_summary = self._fetch_task_summary(app_id, stage_data["stageId"])

            tasks = TaskMetrics(
                task_count=stage_data.get("numTasks", 0),
                median_duration_ms=_percentile_value(task_summary, "executorRunTime", 0.5),
                max_duration_ms=_percentile_value(task_summary, "executorRunTime", 1.0),
                min_duration_ms=_percentile_value(task_summary, "executorRunTime", 0.0),
                total_gc_time_ms=stage_data.get("jvmGcTime", 0),
                total_shuffle_read_bytes=stage_data.get("shuffleReadBytes", 0),
                total_shuffle_write_bytes=stage_data.get("shuffleWriteBytes", 0),
                spill_to_disk_bytes=stage_data.get("diskBytesSpilled", 0),
                spill_to_memory_bytes=stage_data.get("memoryBytesSpilled", 0),
                failed_task_count=stage_data.get("numFailedTasks", 0),
            )

            stages.append(
                StageMetrics(
                    stage_id=stage_data["stageId"],
                    stage_name=stage_data.get("name", f"Stage {stage_data['stageId']}"),
                    duration_ms=stage_data.get("executorRunTime", 0),
                    input_bytes=stage_data.get("inputBytes", 0),
                    output_bytes=stage_data.get("outputBytes", 0),
                    tasks=tasks,
                )
            )

        return stages

    def _fetch_task_summary(self, app_id: str, stage_id: int) -> dict:
        response = self._client.get(
            f"/applications/{app_id}/stages/{stage_id}/0/taskSummary",
            params={"quantiles": "0.0,0.25,0.5,0.75,1.0"},
        )
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        return response.json()

    def _fetch_executors(self, app_id: str) -> ExecutorMetrics:
        response = self._client.get(f"/applications/{app_id}/executors")
        response.raise_for_status()
        executors = response.json()

        non_driver = [e for e in executors if e.get("id") != "driver"]

        return ExecutorMetrics(
            executor_count=len(non_driver),
            peak_memory_bytes=sum(
                e.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0)
                for e in non_driver
            ),
            allocated_memory_bytes=sum(e.get("maxMemory", 0) for e in non_driver),
            total_cpu_time_ms=sum(e.get("totalDuration", 0) for e in non_driver),
            total_run_time_ms=sum(e.get("totalDuration", 0) for e in non_driver),
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "HistoryServerSource":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _percentile_value(summary: dict, metric: str, quantile: float) -> int:
    """Extract a percentile value from History Server task summary response."""
    values = summary.get(metric, [])
    if not values:
        return 0
    quantile_map = {0.0: 0, 0.25: 1, 0.5: 2, 0.75: 3, 1.0: 4}
    idx = quantile_map.get(quantile, 2)
    if idx >= len(values):
        return int(values[-1]) if values else 0
    return int(values[idx])
