from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.model.events import KafkaEnvelope


class KafkaSink:
    def __init__(self, producer: KafkaProducerWrapper, topic: str) -> None:
        self._wrapper: KafkaProducerWrapper = producer
        self._topic: str = topic

    def __enter__(self) -> "KafkaSink":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def send(self, envelope: KafkaEnvelope) -> None:
        key = envelope.payload.get("app_id") or envelope.metadata.message_id
        self._wrapper.send(self._topic, key=key, envelope=envelope)

    def close(self) -> None:
        self._wrapper.close()

    def is_healthy(self) -> bool:
        return self._wrapper.flush(timeout=5.0) == 0
