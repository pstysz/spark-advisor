from __future__ import annotations

import logging
from contextlib import ExitStack, asynccontextmanager
from typing import TYPE_CHECKING

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI

from spark_advisor_hs_poller.config import PollerSettings
from spark_advisor_hs_poller.history_server_client import HistoryServerClient
from spark_advisor_hs_poller.poller import HistoryServerPoller
from spark_advisor_hs_poller.pooling_state import PollingState
from spark_advisor_hs_poller.sink import KafkaJobAnalysisSink
from spark_advisor_shared.config.kafka import KafkaTopicSettings
from spark_advisor_shared.health.fastapi_health import create_health_router
from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.telemetry.setup import init_telemetry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_hs_client: HistoryServerClient | None = None
_producer: KafkaProducerWrapper | None = None


def _check_history_server() -> bool:
    if _hs_client is None:
        return False
    try:
        _hs_client.list_applications(limit=1)
        return True
    except Exception:
        return False


def _check_kafka() -> bool:
    if _producer is None:
        return False
    try:
        remaining = _producer.flush(timeout=5.0)
        return remaining == 0
    except Exception:
        return False


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    global _scheduler, _hs_client, _producer

    settings = PollerSettings()
    topics = KafkaTopicSettings()

    init_telemetry("spark-advisor-hs-poller", settings)

    logging.basicConfig(level=settings.log_level)

    with ExitStack() as stack:
        try:
            _hs_client = stack.enter_context(
                HistoryServerClient(settings.history_server_url, timeout=settings.history_server_timeout)
            )

            _producer = KafkaProducerWrapper(settings.kafka)
            sink = KafkaJobAnalysisSink(_producer, topics.job_analysis_topic)
            cursor = PollingState()

            poller = HistoryServerPoller(
                hs_client=_hs_client,
                sink=sink,
                pool_state=cursor,
                batch_size=settings.batch_size,
            )

            _scheduler = BackgroundScheduler()
            _scheduler.add_job(
                poller.poll,
                "interval",
                seconds=settings.poll_interval_seconds,
                id="hs_poll",
                max_instances=1,
            )
            _scheduler.start()
            logger.info(
                "HS Poller started: url=%s interval=%ds batch=%d",
                settings.history_server_url,
                settings.poll_interval_seconds,
                settings.batch_size,
            )

            yield
        finally:
            if _scheduler is not None:
                _scheduler.shutdown(wait=False)


def create_app() -> FastAPI:
    app = FastAPI(title="spark-advisor-hs-poller", lifespan=lifespan)
    health_router = create_health_router(
        readiness_checks={
            "history_server": _check_history_server,
            "kafka": _check_kafka,
        }
    )
    app.include_router(health_router)
    return app


def main() -> None:
    import uvicorn

    settings = PollerSettings()
    app = create_app()
    uvicorn.run(app, host=settings.server_host, port=settings.server_port)


_app: FastAPI | None = None


def get_app() -> FastAPI:
    global _app
    if _app is None:
        _app = create_app()
    return _app
