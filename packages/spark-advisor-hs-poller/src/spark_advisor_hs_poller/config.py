from pydantic_settings import SettingsConfigDict

from spark_advisor_shared.config.base import BaseServiceSettings


class PollerSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_POLLER_",
        yaml_file="/etc/spark-advisor/hs-poller/config.yaml",
    )

    history_server_url: str = "http://localhost:18080"
    history_server_timeout: float = 30.0
    poll_interval_seconds: int = 60
    batch_size: int = 50
    server_host: str = "0.0.0.0"
    server_port: int = 8080
