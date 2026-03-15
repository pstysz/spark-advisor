from enum import StrEnum

from pydantic_settings import SettingsConfigDict

from spark_advisor_models.config import AiSettings, Thresholds
from spark_advisor_models.defaults import (
    DEFAULT_AI_SETTINGS,
    DEFAULT_THRESHOLDS,
    NATS_ANALYSIS_RESULT_SUBJECT,
    NATS_ANALYSIS_RUN_SUBJECT,
)
from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class ContextKey(StrEnum):
    AI_CLIENT = "ai_client"
    ORCHESTRATOR = "orchestrator"


class AnalyzerNatsSettings(NatsSettings):
    analysis_run_subject: str = NATS_ANALYSIS_RUN_SUBJECT
    analysis_result_subject: str = NATS_ANALYSIS_RESULT_SUBJECT


class AnalyzerSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_ANALYZER_",
        yaml_file="/etc/spark-advisor/analyzer/config.yaml",
    )

    nats: AnalyzerNatsSettings = AnalyzerNatsSettings()
    thresholds: Thresholds = DEFAULT_THRESHOLDS
    ai: AiSettings = DEFAULT_AI_SETTINGS
