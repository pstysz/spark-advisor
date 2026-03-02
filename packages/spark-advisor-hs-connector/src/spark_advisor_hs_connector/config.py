from pydantic_settings import SettingsConfigDict

from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


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
