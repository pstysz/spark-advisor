from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from confluent_kafka import KafkaError, Message, Producer

from spark_advisor_shared.kafka.serde import serialize_message

if TYPE_CHECKING:
    from spark_advisor_shared.config.kafka import KafkaProducerSettings
    from spark_advisor_shared.model.events import KafkaEnvelope

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    def __init__(self, config: KafkaProducerSettings) -> None:
        self._producer = Producer(config.to_confluent_config())

    def __enter__(self) -> KafkaProducerWrapper:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def send(self, topic: str, key: str, envelope: KafkaEnvelope) -> None:
        data = serialize_message(envelope)

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                logger.error("Kafka delivery failed: %s", err)
            else:
                logger.debug("Delivered to %s [%s] @ %s", msg.topic(), msg.partition(), msg.offset())

        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=data,
            callback=on_delivery,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        if self._producer is None:
            return 0
        return self._producer.flush(timeout)

    def close(self) -> None:
        if self._producer is None:
            return
        self._producer.flush(30.0)
        self._producer = None  # type: ignore[assignment]
