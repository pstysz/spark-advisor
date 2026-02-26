from __future__ import annotations

import logging
from contextlib import ExitStack, asynccontextmanager
from typing import TYPE_CHECKING

from apscheduler.schedulers.background import BackgroundScheduler

from spark_advisor_hs_poller.history_server_client import HistoryServerClient
from spark_advisor_hs_poller.poller import HistoryServerPoller
from spark_advisor_hs_poller.pooling_state import PollingState
from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.kafka.sink import KafkaSink
from spark_advisor_shared.telemetry.setup import init_telemetry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from fastapi import FastAPI

    from spark_advisor_hs_poller.config import PollerSettings

logger = logging.getLogger(__name__)


class LifecycleManager:
    def __init__(self, settings: PollerSettings, state: PollingState | None = None) -> None:
        self._scheduler: BackgroundScheduler | None = None
        self._hs_client: HistoryServerClient | None = None
        self._sink: KafkaSink | None = None
        self._settings: PollerSettings = settings
        self._state: PollingState = state or PollingState()

    def check_history_server(self) -> bool:
        if self._hs_client is None:
            return False
        try:
            self._hs_client.list_applications(limit=1)
            return True
        except Exception:
            return False

    def check_kafka(self) -> bool:
        if self._sink is None:
            return False
        try:
            return self._sink.is_healthy()
        except Exception:
            return False

    @asynccontextmanager
    async def lifespan(self, _app: FastAPI) -> AsyncGenerator[None, None]:
        topics = self._settings.kafka_topic_settings

        init_telemetry("spark-advisor-hs-poller", self._settings)
        logging.basicConfig(level=self._settings.log_level)

        with ExitStack() as stack:
            self._hs_client = stack.enter_context(
                HistoryServerClient(self._settings.history_server_url,
                                    timeout=self._settings.history_server_timeout))

            producer_wrapper = stack.enter_context(
                KafkaProducerWrapper(self._settings.kafka_producer_settings))
            self._sink = stack.enter_context(KafkaSink(producer_wrapper, topics.job_analysis_topic))

            poller = HistoryServerPoller(
                hs_client=self._hs_client,
                sink=self._sink,
                pool_state=self._state,
                batch_size=self._settings.batch_size,
            )

            self._scheduler = BackgroundScheduler()
            self._scheduler.add_job(
                poller.poll,
                "interval",
                seconds=self._settings.poll_interval_seconds,
                id="hs_poll",
                max_instances=1,
            )
            self._scheduler.start()
            stack.callback(self._scheduler.shutdown, wait=False)

            logger.info(
                "HS Poller started: url=%s interval=%ds batch=%d",
                self._settings.history_server_url,
                self._settings.poll_interval_seconds,
                self._settings.batch_size,
            )

            yield
