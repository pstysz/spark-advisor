from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import ValidationError

from spark_advisor_shared.model.metrics import JobAnalysis
from spark_advisor_shared.model.events import KafkaEnvelope, MessageMetadata
from spark_advisor_shared.telemetry.setup import get_tracer

if TYPE_CHECKING:
    import threading

    from spark_advisor.advice_orchestrator import AdviceOrchestrator
    from spark_advisor_shared.kafka.sink import KafkaSink
    from spark_advisor_shared.kafka.source import KafkaSource

logger = logging.getLogger(__name__)
tracer = get_tracer(__name__)


class AdviceProcessor:
    def __init__(
            self,
            source: KafkaSource,
            sink: KafkaSink,
            advisor: AdviceOrchestrator,
    ) -> None:
        self._source = source
        self._sink = sink
        self._advisor = advisor

    def run(self, stop_event: threading.Event) -> None:
        logger.info("Advice processor loop started")
        while not stop_event.is_set():
            envelope = self._source.poll(timeout=1.0)
            if envelope is None:
                continue
            try:
                self._process(envelope)
            except ValidationError:
                logger.exception(
                    "Invalid message schema, skipping: %s",
                    envelope.metadata.message_id,
                )

                #ToDo: dlq
            except Exception:
                logger.exception(
                    "Transient error processing %s, will retry",
                    envelope.metadata.message_id,
                )
                continue
            self._source.commit()

    def _process(self, envelope: KafkaEnvelope) -> None:
        with tracer.start_as_current_span("advice_processor.process") as span:
            app_id = envelope.payload.get("app_id", "unknown")
            span.set_attribute("app_id", app_id)

            job = JobAnalysis.model_validate(envelope.payload)
            result = self._advisor.run(job)

            result_envelope = KafkaEnvelope(
                metadata=MessageMetadata(source="spark-advisor"),
                payload=result.model_dump(mode="json"),
            )
            self._sink.send(result_envelope)

            logger.info("Analyzed app %s: %d rule results", app_id, len(result.rule_results))
