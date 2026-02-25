from unittest.mock import MagicMock

from spark_advisor_shared.kafka.source import KafkaSource
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


class TestKafkaSource:
    def test_subscribes_on_init(self) -> None:
        consumer = MagicMock()
        KafkaSource(consumer, "test-topic")
        consumer.subscribe.assert_called_once_with(["test-topic"])

    def test_poll_delegates_to_consumer(self) -> None:
        consumer = MagicMock()
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"app_id": "app-001"},
        )
        consumer.poll.return_value = envelope

        source = KafkaSource(consumer, "test-topic")
        result = source.poll(timeout=0.5)

        assert result is envelope
        consumer.poll.assert_called_once_with(0.5)

    def test_poll_returns_none(self) -> None:
        consumer = MagicMock()
        consumer.poll.return_value = None

        source = KafkaSource(consumer, "test-topic")
        assert source.poll() is None

    def test_commit_delegates(self) -> None:
        consumer = MagicMock()
        source = KafkaSource(consumer, "test-topic")
        source.commit()
        consumer.commit.assert_called_once()

    def test_close_delegates(self) -> None:
        consumer = MagicMock()
        source = KafkaSource(consumer, "test-topic")
        source.close()
        consumer.close.assert_called_once()
