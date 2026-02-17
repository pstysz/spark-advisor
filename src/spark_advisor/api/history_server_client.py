from typing import Any

import httpx
from pydantic import TypeAdapter

from spark_advisor.model import SparkConfig
from spark_advisor.model.metrics import ExecutorMetrics, JobAnalysis, StageMetrics, TaskMetrics
from spark_advisor.model.output import ApplicationSummary


class HistoryServerClient:
    """Usage:
    with HistoryServerClient("http://yarn-master:18080") as client:
        job = client.fetch("application_1234567890_0001")
    """

    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: httpx.Client | None = None

    def __enter__(self) -> "HistoryServerClient":
        if self._client is None:
            self._client = httpx.Client(
                base_url=f"{self._base_url}/api/v1",
                timeout=self._timeout,
                headers={"Accept": "application/json"},
            )
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("HistoryServerClient must be used within 'with' block")
        return self._client

    def list_applications(self, limit: int = 20) -> list[ApplicationSummary]:
        response = self._get_client().get("/applications", params={"limit": limit})
        response.raise_for_status()
        return TypeAdapter(list[ApplicationSummary]).validate_json(response.text)

    def fetch(self, app_id: str) -> JobAnalysis:
        app_info = self._fetch_app_info(app_id)

        attempts = app_info.get("attempts", [])
        latest = attempts[-1] if attempts else {}
        attempt_id: str | None = None
        if "attemptId" in latest:
            attempt_id = str(latest["attemptId"])

        base_path = f"/applications/{app_id}/{attempt_id}" if attempt_id else f"/applications/{app_id}"

        config, spark_version = self._fetch_environment(base_path)
        stages = self._fetch_stages(base_path)
        executors = self._fetch_executors(base_path)

        duration = latest.get("duration", 0)

        return JobAnalysis(
            app_id=app_id,
            app_name=app_info.get("name", ""),
            spark_version=spark_version,
            duration_ms=duration,
            config=config,
            stages=stages,
            executors=executors,
        )

    def _fetch_app_info(self, app_id: str) -> dict[str, Any]:
        response = self._get_client().get(f"/applications/{app_id}")
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def _fetch_environment(self, base_path: str) -> tuple[SparkConfig, str]:
        response = self._get_client().get(f"{base_path}/environment")
        response.raise_for_status()
        env = response.json()

        spark_props: dict[str, str] = {}
        for prop in env.get("sparkProperties", []):
            if len(prop) >= 2:
                spark_props[prop[0]] = prop[1]

        spark_version = env.get("runtime", {}).get("sparkVersion", "")

        return SparkConfig(raw=spark_props), spark_version

    def _fetch_stages(self, base_path: str) -> list[StageMetrics]:
        response = self._get_client().get(
            f"{base_path}/stages",
            params={"status": "complete"},
        )
        response.raise_for_status()

        stages: list[StageMetrics] = []
        for stage_data in response.json():
            task_summary = self._fetch_task_summary(base_path, stage_data["stageId"])

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

    def _fetch_task_summary(self, base_path: str, stage_id: int) -> dict[str, Any]:
        response = self._get_client().get(
            f"{base_path}/stages/{stage_id}/0/taskSummary",
            params={"quantiles": "0.0,0.25,0.5,0.75,1.0"},
        )
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def _fetch_executors(self, base_path: str) -> ExecutorMetrics:
        response = self._get_client().get(f"{base_path}/executors")
        response.raise_for_status()
        executors = response.json()

        non_driver = [e for e in executors if e.get("id") != "driver"]

        return ExecutorMetrics(
            executor_count=len(non_driver),
            peak_memory_bytes=sum(
                e.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0) for e in non_driver
            ),
            allocated_memory_bytes=sum(e.get("maxMemory", 0) for e in non_driver),
            total_cpu_time_ms=None,
            total_run_time_ms=None,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


def _percentile_value(summary: dict[str, Any], metric: str, quantile: float) -> int:
    values = summary.get(metric, [])
    if not values:
        return 0
    quantile_map = {0.0: 0, 0.25: 1, 0.5: 2, 0.75: 3, 1.0: 4}
    idx = quantile_map.get(quantile, 2)
    if idx >= len(values):
        return int(values[-1]) if values else 0
    return int(values[idx])
