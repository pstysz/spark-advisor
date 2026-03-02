from pydantic import BaseModel, ConfigDict
from pydantic_settings import SettingsConfigDict

from spark_advisor_models.settings import BaseServiceSettings, NatsSettings


class GatewayNatsSettings(NatsSettings):
    fetch_subject: str = "fetch.job"
    analyze_subject: str = "analyze.request"
    list_apps_subject: str = "list.applications"
    fetch_timeout: float = 30.0
    analyze_timeout: float = 120.0
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
    max_stored_tasks: int = 1000
