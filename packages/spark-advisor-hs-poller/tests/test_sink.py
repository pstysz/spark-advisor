from unittest.mock import MagicMock

from spark_advisor_hs_poller.sink import KafkaJobAnalysisSink
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


class TestKafkaJobAnalysisSink:
    def test_send_delegates_to_producer(self) -> None:
        producer = MagicMock()
        sink = KafkaJobAnalysisSink(producer=producer, topic="test-topic")

        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="hs-poller"),
            payload={"app_id": "app-001", "duration_ms": 300_000},
        )
        sink.send(envelope)

        producer.send.assert_called_once_with("test-topic", key="app-001", envelope=envelope)

    def test_send_uses_unknown_key_when_no_app_id(self) -> None:
        producer = MagicMock()
        sink = KafkaJobAnalysisSink(producer=producer, topic="test-topic")

        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"some_field": "value"},
        )
        sink.send(envelope)

        producer.send.assert_called_once_with("test-topic", key="unknown", envelope=envelope)

    def test_close_delegates_to_producer(self) -> None:
        producer = MagicMock()
        sink = KafkaJobAnalysisSink(producer=producer, topic="test-topic")
        sink.close()
        producer.close.assert_called_once()
