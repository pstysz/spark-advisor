from enum import StrEnum

from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import NATS_ANALYSIS_SUBMIT_SUBJECT, NATS_FETCH_JOB_SUBJECT
from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class ContextKey(StrEnum):
    POLLER = "poller"
    HS_CLIENT = "hs_client"
    POLLING_TASK = "polling_task"
    POLLING_STATE = "polling_state"


class ConnectorNatsSettings(NatsSettings):
    analysis_submit_subject: str = NATS_ANALYSIS_SUBMIT_SUBJECT
    job_fetch_subject: str = NATS_FETCH_JOB_SUBJECT


class ConnectorSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_CONNECTOR_",
        yaml_file="/etc/spark-advisor/hs-connector/config.yaml",
    )

    nats: ConnectorNatsSettings = ConnectorNatsSettings()
    polling_enabled: bool = False
    history_server_url: str = "http://localhost:18080"
    history_server_timeout: float = 30.0
    poll_interval_seconds: int = 60
    batch_size: int = 50
    max_processed_apps: int = 10_000
    database_url: str = "sqlite+aiosqlite:///data/hs_connector.db"
