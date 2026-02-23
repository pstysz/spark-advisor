from abc import ABC, abstractmethod

from spark_advisor_shared.model.events import KafkaEnvelope


class Sink(ABC):
    @abstractmethod
    def send(self, envelope: KafkaEnvelope) -> None: ...

    @abstractmethod
    def close(self) -> None: ...
