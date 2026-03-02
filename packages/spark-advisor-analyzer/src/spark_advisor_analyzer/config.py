from pydantic_settings import SettingsConfigDict

from spark_advisor_models.config import AiSettings, Thresholds
from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class AnalyzerNatsSettings(NatsSettings):
    request_subject: str = "analyze.request"
    result_subject: str = "analyze.result"


class AnalyzerSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_ANALYZER_",
        yaml_file="/etc/spark-advisor/analyzer/config.yaml",
    )

    nats: AnalyzerNatsSettings = AnalyzerNatsSettings()
    thresholds: Thresholds = Thresholds()
    ai: AiSettings = AiSettings()
