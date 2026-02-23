from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from fastapi import FastAPI

from spark_advisor.config import AnalyzerSettings
from spark_advisor_shared.config.kafka import KafkaTopicSettings
from spark_advisor_shared.health.fastapi_health import create_health_router
from spark_advisor_shared.kafka.producer import KafkaProducerWrapper
from spark_advisor_shared.telemetry.setup import init_telemetry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

logger = logging.getLogger(__name__)

_producer: KafkaProducerWrapper | None = None


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
    global _producer

    settings = AnalyzerSettings()
    _topics = KafkaTopicSettings()

    init_telemetry("spark-advisor", settings)
    logging.basicConfig(level=settings.log_level)

    try:
        _producer = KafkaProducerWrapper(settings.kafka)

        logger.info(
            "Spark Advisor service started: ai_enabled=%s model=%s",
            settings.ai_enabled,
            settings.default_model,
        )

        yield
    finally:
        if _producer is not None:
            _producer.close()


def create_app() -> FastAPI:
    app = FastAPI(title="spark-advisor", lifespan=lifespan)
    health_router = create_health_router(
        readiness_checks={
            "kafka": _check_kafka,
        }
    )
    app.include_router(health_router)
    return app


def main() -> None:
    import uvicorn

    settings = AnalyzerSettings()
    app = create_app()
    uvicorn.run(app, host=settings.server_host, port=settings.server_port)


_app: FastAPI | None = None


def get_app() -> FastAPI:
    global _app
    if _app is None:
        _app = create_app()
    return _app
