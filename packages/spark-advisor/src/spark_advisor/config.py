from pydantic_settings import SettingsConfigDict

from spark_advisor_models.config import AiSettings, Thresholds
from spark_advisor_shared.config.base import BaseServiceSettings

__all__ = ["AiSettings", "AnalyzerSettings", "Thresholds"]


class AnalyzerSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_ANALYZER_",
        yaml_file="/etc/spark-advisor/analyzer/config.yaml",
    )

    server_host: str = "0.0.0.0"
    server_port: int = 8080
    thresholds: Thresholds = Thresholds()
    ai_settings: AiSettings = AiSettings()
