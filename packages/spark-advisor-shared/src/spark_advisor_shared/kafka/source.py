from spark_advisor_shared.kafka.consumer import KafkaConsumerWrapper
from spark_advisor_shared.model.events import KafkaEnvelope


class KafkaSource:
    def __init__(self, consumer: KafkaConsumerWrapper, topic: str) -> None:
        self._wrapper: KafkaConsumerWrapper = consumer
        consumer.subscribe([topic])

    def __enter__(self) -> "KafkaSource":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def poll(self, timeout: float = 1.0) -> KafkaEnvelope | None:
        return self._wrapper.poll(timeout)

    def commit(self) -> None:
        self._wrapper.commit()

    def close(self) -> None:
        self._wrapper.close()

    def is_assigned(self) -> bool:
        return self._wrapper.consumer.assignment() != []

