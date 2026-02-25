from __future__ import annotations

from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError

from spark_advisor_shared.config.kafka import KafkaConsumerSettings
from spark_advisor_shared.kafka.consumer import KafkaConsumerWrapper
from spark_advisor_shared.kafka.serde import serialize_message
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata


def _make_envelope() -> KafkaEnvelope:
    return KafkaEnvelope(
        metadata=MessageMetadata(source="test"),
        payload={"app_id": "app-001"},
    )


class TestKafkaConsumerWrapper:
    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_subscribe_delegates(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        wrapper.subscribe(["topic-a", "topic-b"])

        mock_consumer.subscribe.assert_called_once_with(["topic-a", "topic-b"])

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_returns_none_when_no_message(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer
        mock_consumer.poll.return_value = None

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        assert wrapper.poll(timeout=0.1) is None

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_returns_none_on_partition_eof(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_msg = MagicMock()
        mock_err = MagicMock()
        mock_err.code.return_value = KafkaError._PARTITION_EOF
        mock_msg.error.return_value = mock_err
        mock_consumer.poll.return_value = mock_msg

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        assert wrapper.poll() is None

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_returns_none_on_error(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_msg = MagicMock()
        mock_err = MagicMock()
        mock_err.code.return_value = KafkaError._ALL_BROKERS_DOWN
        mock_msg.error.return_value = mock_err
        mock_consumer.poll.return_value = mock_msg

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        assert wrapper.poll() is None

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_deserializes_envelope(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        envelope = _make_envelope()
        data = serialize_message(envelope)

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = data
        mock_consumer.poll.return_value = mock_msg

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        result = wrapper.poll()

        assert result is not None
        assert result.payload["app_id"] == "app-001"
        assert result.metadata.source == "test"

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_commit_delegates(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        wrapper.commit()

        mock_consumer.commit.assert_called_once_with(asynchronous=False)

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_close_delegates(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        wrapper.close()

        mock_consumer.close.assert_called_once()

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_returns_none_on_invalid_json(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b"not valid json {{"
        mock_consumer.poll.return_value = mock_msg

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        assert wrapper.poll() is None

    @patch("spark_advisor_shared.kafka.consumer.Consumer")
    def test_poll_returns_none_on_schema_mismatch(self, mock_consumer_cls: MagicMock) -> None:
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = b'{"wrong_field": 123}'
        mock_consumer.poll.return_value = mock_msg

        wrapper = KafkaConsumerWrapper(KafkaConsumerSettings())
        assert wrapper.poll() is None
