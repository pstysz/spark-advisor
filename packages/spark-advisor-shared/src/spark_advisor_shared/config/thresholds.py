from pydantic import BaseModel, ConfigDict


class Thresholds(BaseModel):
    model_config = ConfigDict(frozen=True)

    skew_warning_ratio: float = 5.0
    skew_critical_ratio: float = 10.0

    spill_warning_gb: float = 0.1
    spill_critical_gb: float = 1.0

    gc_warning_percent: float = 20.0
    gc_critical_percent: float = 40.0

    target_partition_size_bytes: int = 128 * 1024 * 1024
    partition_ratio_min: float = 0.5
    partition_ratio_max: float = 2.0
    small_partition_size_bytes: int = 10 * 1024 * 1024

    min_cpu_utilization_percent: float = 40.0

    task_failure_warning_count: int = 1
