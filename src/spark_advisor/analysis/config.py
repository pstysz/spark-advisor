from dataclasses import dataclass

DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_MAX_TOKENS = 4096


@dataclass(frozen=True)
class RuleThresholds:
    skew_warning_ratio: float = 5.0
    skew_critical_ratio: float = 10.0

    spill_critical_gb: float = 1.0

    gc_warning_percent: float = 20.0
    gc_critical_percent: float = 40.0

    target_partition_size_bytes: int = 128 * 1024 * 1024
    partition_ratio_min: float = 0.5
    partition_ratio_max: float = 2.0

    executor_idle_percent: float = 60.0
