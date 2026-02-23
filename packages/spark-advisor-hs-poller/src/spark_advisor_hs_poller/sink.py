from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.model.events import KafkaEnvelope
from spark_advisor_shared.sink.base import Sink


class KafkaJobAnalysisSink(Sink):
    def __init__(self, producer: KafkaProducerWrapper, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def send(self, envelope: KafkaEnvelope) -> None:
        app_id = envelope.payload.get("app_id", "unknown")
        self._producer.send(self._topic, key=app_id, envelope=envelope)

    def close(self) -> None:
        self._producer.close()
