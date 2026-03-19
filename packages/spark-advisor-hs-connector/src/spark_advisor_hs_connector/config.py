from enum import StrEnum

from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import NATS_FETCH_JOB_SUBJECT
from spark_advisor_models.settings import BaseConnectorNatsSettings, BaseConnectorSettings


class ContextKey(StrEnum):
    POLLER = "poller"
    HS_CLIENT = "hs_client"
    POLLING_TASK = "polling_task"
    POLLING_STATE = "polling_state"
    SERVICE_NAME = "service_name"


class ConnectorNatsSettings(BaseConnectorNatsSettings):
    job_fetch_subject: str = NATS_FETCH_JOB_SUBJECT


class ConnectorSettings(BaseConnectorSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_CONNECTOR_",
        yaml_file="/etc/spark-advisor/hs-connector/config.yaml",
    )

    service_name: str = "spark-advisor-hs-connector"
    nats: ConnectorNatsSettings = ConnectorNatsSettings()
    history_server_url: str = "http://localhost:18080"
    history_server_timeout: float = 30.0
    database_url: str = "sqlite+aiosqlite:///data/hs_connector.db"
