from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError

from spark_advisor_shared.kafka.serde import deserialize_message
from spark_advisor_shared.model.events import KafkaEnvelope

if TYPE_CHECKING:
    from spark_advisor_shared.config.kafka import KafkaConsumerSettings

logger = logging.getLogger(__name__)


class KafkaConsumerWrapper:
    def __init__(self, config: KafkaConsumerSettings) -> None:
        self._consumer = Consumer(config.to_confluent_config())  # type: ignore[arg-type]

    def subscribe(self, topics: list[str]) -> None:
        self._consumer.subscribe(topics)
        logger.info("Subscribed to topics: %s", topics)

    def poll(self, timeout: float = 1.0) -> KafkaEnvelope | None:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None

        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                logger.debug("Reached end of partition %s [%d]", msg.topic(), msg.partition())
                return None
            logger.error("Consumer error: %s", err)
            return None

        data = msg.value()
        if data is None:
            return None

        try:
            return deserialize_message(data, KafkaEnvelope)
        except Exception:
            logger.exception("Failed to deserialize message, skipping: %s", data[:500])
            return None

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()

    @property
    def consumer(self) -> "Consumer":
        return self._consumer
