from pydantic_settings import BaseSettings, SettingsConfigDict, YamlConfigSettingsSource
from pydantic_settings.sources import PydanticBaseSettingsSource

from spark_advisor_shared.config.kafka import KafkaConsumerSettings, KafkaProducerSettings, KafkaTopicSettings


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_",
        env_file=".env",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    log_level: str = "INFO"
    otel_enabled: bool = True
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    kafka_producer_settings: KafkaProducerSettings = KafkaProducerSettings()
    kafka_consumer_settings: KafkaConsumerSettings = KafkaConsumerSettings()
    kafka_topic_settings: KafkaTopicSettings = KafkaTopicSettings()

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
