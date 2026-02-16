from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


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
    speculative_task_count: int = 0

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
    total_cpu_time_ms: int
    total_run_time_ms: int

    @property
    def memory_utilization_percent(self) -> float:
        if self.allocated_memory_bytes == 0:
            return 0.0
        return (self.peak_memory_bytes / self.allocated_memory_bytes) * 100

    @property
    def cpu_utilization_percent(self) -> float:
        if self.total_run_time_ms == 0:
            return 0.0
        return (self.total_cpu_time_ms / self.total_run_time_ms) * 100


class SparkConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    raw: dict[str, str] = Field(default_factory=dict)

    def get(self, key: str, default: str = "") -> str:
        return self.raw.get(key, default)

    @property
    def executor_memory(self) -> str:
        return self.get("spark.executor.memory", "1g")

    @property
    def executor_cores(self) -> int:
        return int(self.get("spark.executor.cores", "1"))

    @property
    def shuffle_partitions(self) -> int:
        return int(self.get("spark.sql.shuffle.partitions", "200"))

    @property
    def dynamic_allocation_enabled(self) -> bool:
        return self.get("spark.dynamicAllocation.enabled", "false").lower() == "true"

    @property
    def aqe_enabled(self) -> bool:
        return self.get("spark.sql.adaptive.enabled", "false").lower() == "true"


class JobAnalysis(BaseModel):
    model_config = ConfigDict(frozen=True)

    app_id: str
    app_name: str = ""
    spark_version: str = ""
    duration_ms: int
    config: SparkConfig
    stages: list[StageMetrics]
    executors: ExecutorMetrics | None = None
    environment: str = ""
