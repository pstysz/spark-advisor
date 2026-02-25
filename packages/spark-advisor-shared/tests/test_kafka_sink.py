from unittest.mock import MagicMock

from spark_advisor_shared.kafka.sink import KafkaSink
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


class TestKafkaSink:
    def test_send_extracts_app_id_as_key(self) -> None:
        producer = MagicMock()
        sink = KafkaSink(producer, "test-topic")

        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"app_id": "app-001", "duration_ms": 300_000},
        )
        sink.send(envelope)

        producer.send.assert_called_once_with("test-topic", key="app-001", envelope=envelope)

    def test_send_uses_message_id_as_fallback_key(self) -> None:
        producer = MagicMock()
        sink = KafkaSink(producer, "test-topic")

        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"some_field": "value"},
        )
        sink.send(envelope)

        producer.send.assert_called_once_with(
            "test-topic", key=envelope.metadata.message_id, envelope=envelope,
        )

    def test_close_delegates(self) -> None:
        producer = MagicMock()
        sink = KafkaSink(producer, "test-topic")
        sink.close()
        producer.close.assert_called_once()
