import os

import structlog
from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_analyzer.ai.client import AnthropicClient
from spark_advisor_analyzer.config import AnalyzerSettings, ContextKey
from spark_advisor_analyzer.factory import create_analysis_stack
from spark_advisor_analyzer.handlers import router
from spark_advisor_models.logging import configure_logging
from spark_advisor_models.tracing import configure_tracing

settings = AnalyzerSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)

logger = structlog.stdlib.get_logger(__name__)


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.service_name, settings.log_level, json_output=settings.json_log)
    configure_tracing(settings.service_name, settings.otel.endpoint, enabled=settings.otel.enabled)

    ai_client: AnthropicClient | None = None
    if settings.ai.enabled and os.environ.get("ANTHROPIC_API_KEY"):
        ai_client = AnthropicClient(settings.ai.api_timeout)
        ai_client.open()
        app.context.set_global(ContextKey.AI_CLIENT, ai_client)
    elif settings.ai.enabled:
        logger.warning("ANTHROPIC_API_KEY not set — AI analysis disabled")

    orchestrator = create_analysis_stack(
        ai_client=ai_client,
        ai_settings=settings.ai,
        thresholds=settings.thresholds,
    )
    app.context.set_global(ContextKey.ORCHESTRATOR, orchestrator)
    app.context.set_global(ContextKey.SERVICE_NAME, settings.service_name)

    logger.info(
        "Analyzer started: ai_enabled=%s model=%s",
        settings.ai.enabled and ai_client is not None,
        settings.ai.model,
    )


@app.on_shutdown
async def on_shutdown() -> None:
    ai_client: AnthropicClient | None = app.context.get(ContextKey.AI_CLIENT)
    if ai_client is not None:
        ai_client.close()
        logger.info("AnthropicClient closed")


def main() -> None:
    import asyncio

    asyncio.run(app.run())
