import asyncio
import contextlib

import structlog
from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_k8s_connector.client import K8sClient
from spark_advisor_k8s_connector.config import ContextKey, K8sConnectorSettings
from spark_advisor_k8s_connector.handlers import router
from spark_advisor_k8s_connector.poller import K8sPoller
from spark_advisor_k8s_connector.store import K8sPollingStore
from spark_advisor_models.logging import configure_logging
from spark_advisor_models.polling import polling_loop
from spark_advisor_models.tracing import configure_tracing

settings = K8sConnectorSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)

logger = structlog.stdlib.get_logger(__name__)


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.service_name, settings.log_level, json_output=settings.json_log)
    configure_tracing(settings.service_name, settings.otel.endpoint, enabled=settings.otel.enabled)

    client = await K8sClient.create(kubeconfig_path=settings.kubeconfig_path)

    polling_store = K8sPollingStore(database_url=settings.database_url, max_size=settings.max_processed_apps)
    await polling_store.init()

    poller = K8sPoller(client=client, broker=broker, store=polling_store, settings=settings)

    app.context.set_global(ContextKey.CLIENT, client)
    app.context.set_global(ContextKey.POLLER, poller)
    app.context.set_global(ContextKey.POLLING_STORE, polling_store)
    app.context.set_global(ContextKey.SERVICE_NAME, settings.service_name)
    app.context.set_global(ContextKey.SETTINGS, settings)

    logger.info(
        "K8s Connector started: namespaces=%s poll_interval=%ds",
        settings.namespaces,
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

    polling_store: K8sPollingStore | None = app.context.get(ContextKey.POLLING_STORE)
    if polling_store is not None:
        await polling_store.close()
        logger.info("PollingStore closed")

    client: K8sClient | None = app.context.get(ContextKey.CLIENT)
    if client is not None:
        await client.close()
        logger.info("K8sClient closed")


@app.after_startup
async def start_polling() -> None:
    if not settings.polling_enabled:
        logger.info("Polling disabled (SA_K8S_CONNECTOR_POLLING_ENABLED=false)")
        return
    poller: K8sPoller = app.context.get(ContextKey.POLLER)
    polling_task = asyncio.create_task(polling_loop(poller, settings.poll_interval_seconds))
    app.context.set_global(ContextKey.POLLING_TASK, polling_task)


def main() -> None:
    asyncio.run(app.run())
