import asyncio
import contextlib

import structlog
from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_models.logging import configure_logging
from spark_advisor_models.tracing import configure_tracing
from spark_advisor_storage_connector.config import (
    CONNECTOR_FETCH_SUBJECTS,
    ConnectorType,
    ContextKey,
    StorageConnectorSettings,
)
from spark_advisor_storage_connector.connectors.protocol import StorageConnector
from spark_advisor_storage_connector.handlers import create_router
from spark_advisor_storage_connector.poller import StoragePoller
from spark_advisor_storage_connector.store import PollingStore

settings = StorageConnectorSettings()
broker = NatsBroker(settings.nats.url)
_fetch_subject = CONNECTOR_FETCH_SUBJECTS[settings.connector_type]
broker.include_router(create_router(_fetch_subject, settings.connector_type))
app = FastStream(broker)

logger = structlog.stdlib.get_logger(__name__)


def _create_connector(s: StorageConnectorSettings) -> StorageConnector:
    match s.connector_type:
        case ConnectorType.HDFS:
            from spark_advisor_storage_connector.connectors.hdfs import HdfsConnector
            return HdfsConnector(s.hdfs)
        case ConnectorType.S3:
            from spark_advisor_storage_connector.connectors.s3 import S3Connector
            return S3Connector(s.s3)
        case ConnectorType.GCS:
            from spark_advisor_storage_connector.connectors.gcs import GcsConnector
            return GcsConnector(s.gcs)


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.service_name, settings.log_level, json_output=settings.json_log)
    configure_tracing(settings.service_name, settings.otel.endpoint, enabled=settings.otel.enabled)

    connector = _create_connector(settings)

    polling_store = PollingStore(database_url=settings.database_url, max_size=settings.max_processed_apps)
    await polling_store.init()

    poller = StoragePoller(
        connector=connector,
        broker=broker,
        publish_subject=settings.nats.analysis_submit_subject,
        store=polling_store,
    )

    app.context.set_global(ContextKey.CONNECTOR, connector)
    app.context.set_global(ContextKey.POLLER, poller)
    app.context.set_global(ContextKey.POLLING_STORE, polling_store)
    app.context.set_global(ContextKey.SERVICE_NAME, settings.service_name)

    logger.info(
        "Storage Connector started: connector=%s poll_interval=%ds",
        settings.connector_type,
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

    polling_store: PollingStore | None = app.context.get(ContextKey.POLLING_STORE)
    if polling_store is not None:
        await polling_store.close()
        logger.info("PollingStore database closed")

    connector: StorageConnector | None = app.context.get(ContextKey.CONNECTOR)
    if connector is not None:
        await connector.close()
        logger.info("StorageConnector closed")


@app.after_startup
async def start_polling() -> None:
    if not settings.polling_enabled:
        logger.info("Polling disabled (SA_STORAGE_POLLING_ENABLED=false)")
        return
    poller: StoragePoller = app.context.get(ContextKey.POLLER)
    polling_task = asyncio.create_task(_polling_loop(poller, settings.poll_interval_seconds))
    app.context.set_global(ContextKey.POLLING_TASK, polling_task)


async def _polling_loop(poller: StoragePoller, interval: int) -> None:
    while True:
        try:
            count = await poller.poll()
            if count > 0:
                logger.info("Published %d new event logs", count)
        except Exception:
            logger.exception("Poll cycle failed")
        await asyncio.sleep(interval)


def main() -> None:
    asyncio.run(app.run())
