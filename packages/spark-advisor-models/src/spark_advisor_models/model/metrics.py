from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from spark_advisor_models.model.spark_config import SparkConfig


class Quantiles(BaseModel):
    model_config = ConfigDict(frozen=True)

    min: float = 0.0
    p25: float = 0.0
    median: float = 0.0
    p75: float = 0.0
    max: float = 0.0

    @property
    def iqr(self) -> float:
        return max(0.0, self.p75 - self.p25)

    @property
    def skew_ratio(self) -> float:
        if self.median == 0:
            return 0.0
        return self.max / self.median


class IOQuantiles(BaseModel):
    model_config = ConfigDict(frozen=True)

    bytes: Quantiles = Field(default_factory=Quantiles)
    records: Quantiles = Field(default_factory=Quantiles)


class ShuffleReadQuantiles(BaseModel):
    model_config = ConfigDict(frozen=True)

    read_bytes: Quantiles = Field(default_factory=Quantiles)
    read_records: Quantiles = Field(default_factory=Quantiles)

    remote_blocks_fetched: Quantiles = Field(default_factory=Quantiles)
    local_blocks_fetched: Quantiles = Field(default_factory=Quantiles)
    total_blocks_fetched: Quantiles = Field(default_factory=Quantiles)

    fetch_wait_time: Quantiles = Field(default_factory=Quantiles)
    remote_bytes_read: Quantiles = Field(default_factory=Quantiles)
    remote_bytes_read_to_disk: Quantiles = Field(default_factory=Quantiles)


class ShuffleWriteQuantiles(BaseModel):
    model_config = ConfigDict(frozen=True)

    write_bytes: Quantiles = Field(default_factory=Quantiles)
    write_records: Quantiles = Field(default_factory=Quantiles)
    write_time: Quantiles = Field(default_factory=Quantiles)


class TaskMetricsDistributions(BaseModel):
    model_config = ConfigDict(frozen=True)

    duration: Quantiles = Field(default_factory=Quantiles)

    executor_deserialize_time: Quantiles = Field(default_factory=Quantiles)
    executor_deserialize_cpu_time: Quantiles = Field(default_factory=Quantiles)

    executor_run_time: Quantiles = Field(default_factory=Quantiles)
    executor_cpu_time: Quantiles = Field(default_factory=Quantiles)

    scheduler_delay: Quantiles = Field(default_factory=Quantiles)
    getting_result_time: Quantiles = Field(default_factory=Quantiles)
    result_serialization_time: Quantiles = Field(default_factory=Quantiles)

    jvm_gc_time: Quantiles = Field(default_factory=Quantiles)
    result_size: Quantiles = Field(default_factory=Quantiles)

    peak_execution_memory: Quantiles = Field(default_factory=Quantiles)
    memory_bytes_spilled: Quantiles = Field(default_factory=Quantiles)
    disk_bytes_spilled: Quantiles = Field(default_factory=Quantiles)

    input_metrics: IOQuantiles = Field(default_factory=IOQuantiles)
    output_metrics: IOQuantiles = Field(default_factory=IOQuantiles)
    shuffle_read_metrics: ShuffleReadQuantiles = Field(default_factory=ShuffleReadQuantiles)
    shuffle_write_metrics: ShuffleWriteQuantiles = Field(default_factory=ShuffleWriteQuantiles)


class TaskMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    task_count: int = 0
    distributions: TaskMetricsDistributions = Field(default_factory=TaskMetricsDistributions)

    @property
    def duration_skew_ratio(self) -> float:
        return self.distributions.executor_run_time.skew_ratio


class StageMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_id: int
    stage_name: str

    sum_executor_run_time_ms: int = 0
    total_gc_time_ms: int = 0

    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    spill_to_disk_bytes: int = 0
    spill_to_memory_bytes: int = 0
    failed_task_count: int = 0

    input_bytes: int = 0
    input_records: int = 0
    output_bytes: int = 0
    output_records: int = 0
    shuffle_read_records: int = 0
    shuffle_write_records: int = 0
    killed_task_count: int = 0

    tasks: TaskMetrics = Field(default_factory=TaskMetrics)

    @property
    def gc_time_percent(self) -> float:
        if self.sum_executor_run_time_ms == 0:
            return 0.0
        return (self.total_gc_time_ms / self.sum_executor_run_time_ms) * 100


class ExecutorMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    executor_count: int
    peak_memory_bytes_sum: int
    allocated_memory_bytes_sum: int
    total_task_time_ms: int = 0
    total_gc_time_ms: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    failed_tasks: int = 0
    total_cores: int = 0

    @property
    def memory_utilization_percent(self) -> float:
        if self.allocated_memory_bytes_sum == 0:
            return 0.0
        return (self.peak_memory_bytes_sum / self.allocated_memory_bytes_sum) * 100

    def slot_utilization_percent(self, duration_ms: int, executor_cores: int) -> float | None:
        if self.executor_count == 0 or duration_ms <= 0 or executor_cores <= 0:
            return None
        total_slot_time = self.executor_count * executor_cores * duration_ms
        return (self.total_task_time_ms / total_slot_time) * 100


class JobAnalysis(BaseModel):
    model_config = ConfigDict(frozen=True)

    app_id: str
    app_name: str = ""
    spark_version: str = ""
    duration_ms: int
    config: SparkConfig
    stages: list[StageMetrics]
    executors: ExecutorMetrics | None = None
