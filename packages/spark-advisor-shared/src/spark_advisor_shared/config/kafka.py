from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class KafkaTopicSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_analysis_topic: str = "spark-advisor.job-analysis"
    analysis_result_topic: str = "spark-advisor.analysis-result"
    dead_letter_topic: str = "spark-advisor.dead-letter"

    job_analysis_partitions: int = Field(default=12, ge=1)
    analysis_result_partitions: int = Field(default=12, ge=1)
    dead_letter_partitions: int = Field(default=3, ge=1)


class KafkaConsumerSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    group_id: str = Field(default="spark-advisor", min_length=1)
    auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest"
    enable_auto_commit: bool = False
    max_poll_interval_ms: int = Field(default=300_000, ge=1)
    session_timeout_ms: int = Field(default=45_000, ge=1)

    def to_confluent_config(self) -> dict[str, str | int | float | bool]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "session.timeout.ms": self.session_timeout_ms,
        }


class KafkaProducerSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str = Field(default="localhost:9092", min_length=1)
    acks: Literal["all", "0", "1", "-1"] = "all"
    retries: int = Field(default=3, ge=0)
    linger_ms: int = Field(default=10, ge=0)
    compression_type: Literal["gzip", "snappy", "lz4", "zstd", "none"] | None = None

    def to_confluent_config(self) -> dict[str, str | int | float | bool]:
        config: dict[str, str | int | float | bool] = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "linger.ms": self.linger_ms,
        }
        if self.compression_type is not None:
            config["compression.type"] = self.compression_type
        return config
