import pytest
from pydantic import ValidationError

from spark_advisor_shared.config.kafka import KafkaConsumerSettings, KafkaProducerSettings


class TestKafkaConsumerSettings:
    def test_defaults(self) -> None:
        settings = KafkaConsumerSettings()
        assert settings.group_id == "spark-advisor"
        assert settings.auto_offset_reset == "earliest"
        assert settings.enable_auto_commit is False

    def test_to_confluent_config(self) -> None:
        settings = KafkaConsumerSettings(
            bootstrap_servers="broker:9092",
            group_id="test-group",
        )
        config = settings.to_confluent_config()
        assert config["bootstrap.servers"] == "broker:9092"
        assert config["group.id"] == "test-group"
        assert config["auto.offset.reset"] == "earliest"
        assert config["enable.auto.commit"] is False
        assert config["max.poll.interval.ms"] == 300_000
        assert config["session.timeout.ms"] == 45_000

    def test_rejects_invalid_auto_offset_reset(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerSettings(auto_offset_reset="banana")

    def test_rejects_empty_bootstrap_servers(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerSettings(bootstrap_servers="")

    def test_rejects_negative_max_poll_interval(self) -> None:
        with pytest.raises(ValidationError):
            KafkaConsumerSettings(max_poll_interval_ms=-1)


class TestKafkaProducerSettings:
    def test_to_confluent_config(self) -> None:
        settings = KafkaProducerSettings()
        config = settings.to_confluent_config()
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["acks"] == "all"

    def test_compression_type_included_when_set(self) -> None:
        settings = KafkaProducerSettings(compression_type="lz4")
        config = settings.to_confluent_config()
        assert config["compression.type"] == "lz4"

    def test_compression_type_excluded_when_none(self) -> None:
        settings = KafkaProducerSettings()
        config = settings.to_confluent_config()
        assert "compression.type" not in config

    def test_rejects_invalid_acks(self) -> None:
        with pytest.raises(ValidationError):
            KafkaProducerSettings(acks="two")

    def test_rejects_negative_retries(self) -> None:
        with pytest.raises(ValidationError):
            KafkaProducerSettings(retries=-1)

    def test_rejects_invalid_compression_type(self) -> None:
        with pytest.raises(ValidationError):
            KafkaProducerSettings(compression_type="brotli")
