from enum import StrEnum

from pydantic import BaseModel, ConfigDict
from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import (
    NATS_ANALYSIS_RUN_AGENT_SUBJECT,
    NATS_ANALYSIS_RUN_SUBJECT,
    NATS_ANALYSIS_SUBMIT_SUBJECT,
    NATS_APPLICATIONS_LIST_SUBJECT,
    NATS_FETCH_JOB_SUBJECT,
)
from spark_advisor_models.model import AnalysisMode
from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class StateKey(StrEnum):
    NC = "nc"
    SETTINGS = "settings"
    TASK_MANAGER = "task_manager"
    TASK_EXECUTOR = "task_executor"
    CONNECTION_MANAGER = "connection_manager"


class GatewayNatsSettings(NatsSettings):
    job_fetch_subject: str = NATS_FETCH_JOB_SUBJECT
    analysis_run_subject: str = NATS_ANALYSIS_RUN_SUBJECT
    analysis_run_agent_subject: str = NATS_ANALYSIS_RUN_AGENT_SUBJECT
    apps_list_subject: str = NATS_APPLICATIONS_LIST_SUBJECT
    fetch_timeout: float = 30.0
    analyze_timeout: float = 120.0
    analyze_agent_timeout: float = 300.0
    list_apps_timeout: float = 10.0
    analysis_submit_subject: str = NATS_ANALYSIS_SUBMIT_SUBJECT
    polling_analysis_mode: AnalysisMode = AnalysisMode.AI


class ServerSettings(BaseModel):
    model_config = ConfigDict(frozen=True)
    host: str = "0.0.0.0"
    port: int = 8080


class GatewaySettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_GATEWAY_",
        yaml_file="/etc/spark-advisor/gateway/config.yaml",
    )

    server: ServerSettings = ServerSettings()
    nats: GatewayNatsSettings = GatewayNatsSettings()
    database_url: str = "sqlite+aiosqlite:///data/spark_advisor.db"
    ws_heartbeat_interval: float = 30.0
