from pydantic import BaseModel, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict, YamlConfigSettingsSource
from pydantic_settings.sources import PydanticBaseSettingsSource


class OtelSettings(BaseModel):
    model_config = ConfigDict(frozen=True)
    enabled: bool = False
    endpoint: str = "http://localhost:4317"


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_",
        env_file=".env",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    service_name: str = "spark-advisor"
    log_level: str = "INFO"
    json_log: bool = False
    otel: OtelSettings = OtelSettings()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
        )


class NatsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)
    url: str = "nats://localhost:4222"
