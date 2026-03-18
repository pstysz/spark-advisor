import asyncio
import contextlib

import structlog
from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_hs_connector.config import ConnectorSettings, ContextKey
from spark_advisor_hs_connector.handlers import router
from spark_advisor_hs_connector.history_server.client import HistoryServerClient
from spark_advisor_hs_connector.history_server.poller import HistoryServerPoller
from spark_advisor_hs_connector.store import PollingStore
from spark_advisor_models.logging import configure_logging
from spark_advisor_models.tracing import configure_tracing

settings = ConnectorSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)

logger = structlog.stdlib.get_logger(__name__)


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.service_name, settings.log_level, json_output=settings.json_log)
    configure_tracing(settings.service_name, settings.otel.endpoint, enabled=settings.otel.enabled)

    if settings.otel.enabled:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()

    hs_client = HistoryServerClient(settings.history_server_url, settings.history_server_timeout)
    hs_client.open()

    polling_state = PollingStore(database_url=settings.database_url, max_size=settings.max_processed_apps)
    await polling_state.init()

    poller = HistoryServerPoller(
        hs_client=hs_client,
        broker=broker,
        publish_subject=settings.nats.analysis_submit_subject,
        store=polling_state,
        batch_size=settings.batch_size,
    )

    app.context.set_global(ContextKey.POLLER, poller)
    app.context.set_global(ContextKey.HS_CLIENT, hs_client)
    app.context.set_global(ContextKey.POLLING_STATE, polling_state)
    app.context.set_global(ContextKey.SERVICE_NAME, settings.service_name)

    logger.info(
        "HS Connector started: history_server=%s poll_interval=%ds",
        settings.history_server_url,
        settings.poll_interval_seconds,
    )


@app.on_shutdown
async def on_shutdown() -> None:
    polling_task: asyncio.Task[None] | None = app.context.get(ContextKey.POLLING_TASK)
    if polling_task and not polling_task.done():
        polling_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await polling_task
        logger.info("Polling task cancelled")

    polling_state: PollingStore | None = app.context.get(ContextKey.POLLING_STATE)
    if polling_state is not None:
        await polling_state.close()
        logger.info("PollingState database closed")

    hs_client: HistoryServerClient | None = app.context.get(ContextKey.HS_CLIENT)
    if hs_client is not None:
        hs_client.close()
        logger.info("HistoryServerClient closed")


@app.after_startup
async def start_polling() -> None:
    if not settings.polling_enabled:
        logger.info("Polling disabled (SA_CONNECTOR_POLLING_ENABLED=false)")
        return
    poller: HistoryServerPoller = app.context.get(ContextKey.POLLER)
    polling_task = asyncio.create_task(_polling_loop(poller, settings.poll_interval_seconds))
    app.context.set_global(ContextKey.POLLING_TASK, polling_task)


async def _polling_loop(poller: HistoryServerPoller, interval: int) -> None:
    while True:
        try:
            count = await poller.poll()
            if count > 0:
                logger.info("Published %d new jobs", count)
        except Exception:
            logger.exception("Poll cycle failed")
        await asyncio.sleep(interval)


def main() -> None:
    asyncio.run(app.run())
