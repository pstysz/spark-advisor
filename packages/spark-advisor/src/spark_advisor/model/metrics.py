from pydantic import BaseModel, ConfigDict

from spark_advisor.model.spark_config import SparkConfig


class TaskMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_count: int
    median_duration_ms: int
    max_duration_ms: int
    min_duration_ms: int
    total_gc_time_ms: int
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    spill_to_disk_bytes: int = 0
    spill_to_memory_bytes: int = 0
    failed_task_count: int = 0

    @property
    def skew_ratio(self) -> float:
        if self.median_duration_ms == 0:
            return 0.0
        return self.max_duration_ms / self.median_duration_ms

    @property
    def gc_time_percent(self) -> float:
        total_time = self.median_duration_ms * self.task_count
        if total_time == 0:
            return 0.0
        return (self.total_gc_time_ms / total_time) * 100


class StageMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_id: int
    stage_name: str
    duration_ms: int
    input_bytes: int = 0
    output_bytes: int = 0
    tasks: TaskMetrics


class ExecutorMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    executor_count: int
    peak_memory_bytes: int
    allocated_memory_bytes: int
    total_cpu_time_ms: int | None = None
    total_run_time_ms: int | None = None

    @property
    def memory_utilization_percent(self) -> float:
        if self.allocated_memory_bytes == 0:
            return 0.0
        return (self.peak_memory_bytes / self.allocated_memory_bytes) * 100

    @property
    def cpu_utilization_percent(self) -> float | None:
        if self.total_cpu_time_ms is None or self.total_run_time_ms is None:
            return None
        if self.total_run_time_ms == 0:
            return 0.0
        return (self.total_cpu_time_ms / self.total_run_time_ms) * 100


class JobAnalysis(BaseModel):
    model_config = ConfigDict(frozen=True)

    app_id: str
    app_name: str = ""
    spark_version: str = ""
    duration_ms: int
    config: SparkConfig
    stages: list[StageMetrics]
    executors: ExecutorMetrics | None = None
