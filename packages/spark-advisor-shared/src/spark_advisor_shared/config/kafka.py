from pydantic import BaseModel, ConfigDict


class KafkaTopicSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_analysis_topic: str = "spark-advisor.job-analysis"
    analysis_result_topic: str = "spark-advisor.analysis-result"
    dead_letter_topic: str = "spark-advisor.dead-letter"

    job_analysis_partitions: int = 12
    analysis_result_partitions: int = 12
    dead_letter_partitions: int = 3


class KafkaProducerSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str = "localhost:9092"
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 10
    compression_type: str | None = None

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
