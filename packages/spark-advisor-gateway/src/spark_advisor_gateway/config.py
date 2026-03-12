from enum import StrEnum

from pydantic import BaseModel, ConfigDict
from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import (
    NATS_ANALYZE_AGENT_REQUEST_SUBJECT,
    NATS_ANALYZE_REQUEST_SUBJECT,
    NATS_FETCH_JOB_SUBJECT,
    NATS_LIST_APPLICATIONS_SUBJECT,
)
from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class StateKey(StrEnum):
    NC = "nc"
    SETTINGS = "settings"
    TASK_MANAGER = "task_manager"
    TASK_EXECUTOR = "task_executor"


class GatewayNatsSettings(NatsSettings):
    fetch_job_subject: str = NATS_FETCH_JOB_SUBJECT
    analyze_request_subject: str = NATS_ANALYZE_REQUEST_SUBJECT
    analyze_agent_request_subject: str = NATS_ANALYZE_AGENT_REQUEST_SUBJECT
    list_applications_subject: str = NATS_LIST_APPLICATIONS_SUBJECT
    fetch_timeout: float = 30.0
    analyze_timeout: float = 120.0
    analyze_agent_timeout: float = 300.0
    list_apps_timeout: float = 10.0


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
    task_stream_timeout: float = 120.0
    task_poll_interval: float = 0.5
