from __future__ import annotations

import logging

from confluent_kafka import KafkaError, Message, Producer

from spark_advisor_shared.config.kafka import KafkaProducerSettings
from spark_advisor_shared.kafka.serde import serialize_message
from spark_advisor_shared.model.events import KafkaEnvelope

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    def __init__(self, config: KafkaProducerSettings) -> None:
        self._producer = Producer(config.to_confluent_config())

    def send(self, topic: str, key: str, envelope: KafkaEnvelope) -> None:
        data = serialize_message(envelope)
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=data,
            callback=_delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.flush(30.0)


def _delivery_callback(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%s] @ %s", msg.topic(), msg.partition(), msg.offset())
