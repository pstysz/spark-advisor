from __future__ import annotations

import logging
from contextlib import ExitStack, asynccontextmanager
from typing import TYPE_CHECKING

from spark_advisor.advice_orchestrator import AdviceOrchestrator
from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.analysis.advice_processor import AdviceProcessor
from spark_advisor.api.anthropic_client import AnthropicClient
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold
from spark_advisor_shared.kafka.consumer import KafkaConsumerWrapper
from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.kafka.sink import KafkaSink
from spark_advisor_shared.kafka.source import KafkaSource
from spark_advisor_shared.telemetry.setup import init_telemetry
from spark_advisor_shared.util.threading import background_worker

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi import FastAPI

    from spark_advisor.config import AnalyzerSettings

logger = logging.getLogger(__name__)


class LifecycleManager:
    def __init__(self, settings: AnalyzerSettings) -> None:
        self._source: KafkaSource | None = None
        self._sink: KafkaSink | None = None
        self._settings: AnalyzerSettings = settings

    def check_kafka(self) -> bool:
        if self._sink is None or self._source is None:
            return False
        try:
            return self._sink.is_healthy() and self._source.is_assigned()
        except Exception:
            return False

    @asynccontextmanager
    async def lifespan(self, _app: FastAPI) -> AsyncGenerator[None, None]:
        topics = self._settings.kafka_topic_settings

        init_telemetry("spark-advisor", self._settings)
        logging.basicConfig(level=self._settings.log_level)

        with ExitStack() as stack:
            consumer_wrapper = KafkaConsumerWrapper(self._settings.kafka_consumer_settings)
            self._source = stack.enter_context(KafkaSource(consumer_wrapper, topics.job_analysis_topic))

            producer_wrapper = stack.enter_context(
                KafkaProducerWrapper(self._settings.kafka_producer_settings))
            self._sink = stack.enter_context(KafkaSink(producer_wrapper, topics.analysis_result_topic))

            advisor = self._build_advisor(self._settings, stack)
            processor = AdviceProcessor(self._source, self._sink, advisor)
            stack.enter_context(background_worker(processor.run, name="advice-processor"))

            logger.info(
                "Spark Advisor service started: ai_enabled=%s model=%s",
                self._settings.ai_settings.enabled,
                self._settings.ai_settings.model,
            )

            yield

    @staticmethod
    def _build_advisor(settings: AnalyzerSettings, stack: ExitStack) -> AdviceOrchestrator:
        static = StaticAnalysisService(rules_for_threshold(settings.thresholds))

        llm_service: LlmAnalysisService | None = None
        if settings.ai_settings.enabled:
            try:
                client = stack.enter_context(AnthropicClient(settings.ai_settings.api_timeout))
                llm_service = LlmAnalysisService(client, settings.ai_settings, settings.thresholds)
            except ValueError:
                logger.warning("ANTHROPIC_API_KEY not set — AI analysis disabled")

        return AdviceOrchestrator(static, llm_service)
