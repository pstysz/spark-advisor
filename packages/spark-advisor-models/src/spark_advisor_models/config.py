from pydantic import BaseModel, ConfigDict


class Thresholds(BaseModel):
    model_config = ConfigDict(frozen=True)

    skew_warning_ratio: float = 5.0
    skew_critical_ratio: float = 10.0

    spill_warning_gb: float = 0.1
    spill_critical_gb: float = 1.0

    gc_warning_percent: float = 20.0
    gc_critical_percent: float = 40.0
    gc_target_percent: float = 10.0

    target_partition_size_bytes: int = 128 * 1024 * 1024
    partition_ratio_min: float = 0.5
    partition_ratio_max: float = 3.0

    min_slot_utilization_percent: float = 40.0

    task_failure_warning_count: int = 1

    scheduler_delay_ms: int = 100

    small_file_threshold_bytes: int = 10 * 1024 * 1024
    broadcast_join_default_bytes: int = 10 * 1024 * 1024
    memory_overhead_gc_threshold_percent: float = 20.0
    memory_overhead_mem_utilization_percent: float = 80.0
    task_failure_critical_count: int = 10
    min_slot_utilization_critical_percent: float = 20.0
    small_file_critical_bytes: int = 1 * 1024 * 1024

    memory_utilization_critical_percent: float = 95.0

    driver_memory_min_mb: int = 512
    driver_memory_max_mb: int = 16384
    driver_large_cluster_executor_count: int = 50
    driver_large_cluster_min_memory_mb: int = 2048

    memory_underutilization_percent: float = 40.0

    min_duplicate_stages_for_warning: int = 5

    shuffle_volume_absolute_gb: float = 50.0
    shuffle_ratio_warning: float = 3.0

    input_skew_warning_ratio: float = 5.0
    input_skew_critical_ratio: float = 10.0

    gc_min_stage_runtime_ms: int = 60_000

    idle_min_job_duration_ms: int = 300_000

    min_task_count_for_skew: int = 10
    spill_negligible_ratio: float = 0.01


class AiSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = True
    model: str = "claude-sonnet-4-6"
    api_timeout: float = 90.0
    max_tokens: int = 4096
    max_agent_iterations: int = 10
