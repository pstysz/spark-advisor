from enum import StrEnum

from pydantic_settings import SettingsConfigDict

from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class ContextKey(StrEnum):
    POLLER = "poller"
    HS_CLIENT = "hs_client"
    POLLING_TASK = "polling_task"


class ConnectorNatsSettings(NatsSettings):
    publish_subject: str = "analyze.request"
    fetch_subject: str = "fetch.job"


class ConnectorSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_CONNECTOR_",
        yaml_file="/etc/spark-advisor/hs-connector/config.yaml",
    )

    nats: ConnectorNatsSettings = ConnectorNatsSettings()
    history_server_url: str = "http://localhost:18080"
    history_server_timeout: float = 30.0
    poll_interval_seconds: int = 60
    batch_size: int = 50
    max_processed_apps: int = 10_000
