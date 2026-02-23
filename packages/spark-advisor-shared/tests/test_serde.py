from spark_advisor_shared.kafka.serde import deserialize_message, serialize_message
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


class TestSerde:
    def test_serialize_returns_bytes(self) -> None:
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={"app_id": "app-001"},
        )
        data = serialize_message(envelope)
        assert isinstance(data, bytes)

    def test_roundtrip_envelope(self) -> None:
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="hs-poller"),
            payload={"app_id": "app-002", "duration_ms": 300_000, "stages": []},
        )
        data = serialize_message(envelope)
        restored = deserialize_message(data, KafkaEnvelope)
        assert restored.payload["app_id"] == "app-002"
        assert restored.payload["duration_ms"] == 300_000
        assert restored.metadata.source == "hs-poller"

    def test_roundtrip_preserves_metadata(self) -> None:
        meta = MessageMetadata(source="core", trace_id="abc123", span_id="def456")
        envelope = KafkaEnvelope(metadata=meta, payload={"key": "value"})
        data = serialize_message(envelope)
        restored = deserialize_message(data, KafkaEnvelope)
        assert restored.metadata.trace_id == "abc123"
        assert restored.metadata.span_id == "def456"
        assert restored.metadata.message_id == meta.message_id

    def test_roundtrip_nested_payload(self) -> None:
        envelope = KafkaEnvelope(
            metadata=MessageMetadata(source="test"),
            payload={
                "job": {"app_id": "app-003", "stages": [{"stage_id": 0}]},
                "rule_results": [{"rule_id": "data_skew", "severity": "WARNING"}],
            },
        )
        data = serialize_message(envelope)
        restored = deserialize_message(data, KafkaEnvelope)
        assert restored.payload["job"]["app_id"] == "app-003"
        assert restored.payload["rule_results"][0]["rule_id"] == "data_skew"
