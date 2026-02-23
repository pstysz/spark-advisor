from spark_advisor_shared.kafka.serde import deserialize_message, serialize_message
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


class TestMessageMetadata:
    def test_defaults(self) -> None:
        meta = MessageMetadata(source="hs-poller")
        assert meta.source == "hs-poller"
        assert meta.message_id != ""
        assert meta.timestamp is not None

    def test_unique_ids(self) -> None:
        m1 = MessageMetadata(source="test")
        m2 = MessageMetadata(source="test")
        assert m1.message_id != m2.message_id


class TestKafkaEnvelope:
    def test_creation(self) -> None:
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="hs-poller"),
            payload={"app_id": "app-001", "app_name": "TestJob"},
        )
        assert envelope.payload["app_id"] == "app-001"
        assert envelope.metadata.source == "hs-poller"

    def test_serde_roundtrip(self) -> None:
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="core"),
            payload={"app_id": "app-002", "duration_ms": 300_000},
        )
        data = serialize_message(envelope)
        restored = deserialize_message(data, KafkaEnvelope)
        assert restored.payload["app_id"] == "app-002"
        assert restored.metadata.source == "core"

    def test_frozen(self) -> None:
        import pytest
        from pydantic import ValidationError

        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"key": "value"},
        )
        with pytest.raises(ValidationError):
            envelope.metadata = MessageMetadata(source="changed")  # type: ignore[misc]
