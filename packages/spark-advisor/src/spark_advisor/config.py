from pydantic import BaseModel, ConfigDict
from pydantic_settings import SettingsConfigDict

from spark_advisor_shared.config.base import BaseServiceSettings


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
    partition_ratio_max: float = 2.0

    min_slot_utilization_percent: float = 40.0

    task_failure_warning_count: int = 1

    scheduler_delay_ms: int = 100


class AiSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = True
    model: str = "claude-sonnet-4-5"
    api_timeout: float = 90.0
    max_tokens: int = 4096


class AnalyzerSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_ANALYZER_",
        yaml_file="/etc/spark-advisor/analyzer/config.yaml",
    )

    server_host: str = "0.0.0.0"
    server_port: int = 8080
    thresholds: Thresholds = Thresholds()
    ai_settings: AiSettings = AiSettings()
