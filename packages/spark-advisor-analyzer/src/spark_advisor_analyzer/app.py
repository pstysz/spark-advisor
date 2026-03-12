import logging
import os

from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_analyzer.ai.client import AnthropicClient
from spark_advisor_analyzer.config import AnalyzerSettings, ContextKey
from spark_advisor_analyzer.factory import create_analysis_stack
from spark_advisor_analyzer.handlers import router

settings = AnalyzerSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger(__name__)

    client: AnthropicClient | None = None
    if settings.ai.enabled and os.environ.get("ANTHROPIC_API_KEY"):
        client = AnthropicClient(settings.ai.api_timeout)
        client.open()
        app.context.set_global(ContextKey.AI_CLIENT, client)
    elif settings.ai.enabled:
        logger.warning("ANTHROPIC_API_KEY not set — AI analysis disabled")

    orchestrator = create_analysis_stack(
        client=client,
        ai_settings=settings.ai,
        thresholds=settings.thresholds,
    )
    app.context.set_global(ContextKey.ORCHESTRATOR, orchestrator)

    logger.info(
        "Analyzer started: ai_enabled=%s model=%s",
        settings.ai.enabled and client is not None,
        settings.ai.model,
    )


@app.on_shutdown
async def on_shutdown() -> None:
    ai_client: AnthropicClient | None = app.context.get(ContextKey.AI_CLIENT)
    if ai_client is not None:
        ai_client.close()
        logging.getLogger(__name__).info("AnthropicClient closed")


def main() -> None:
    import asyncio

    asyncio.run(app.run())
