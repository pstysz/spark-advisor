import asyncio
import logging

from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_hs_connector.config import ConnectorSettings
from spark_advisor_hs_connector.history_server_client import HistoryServerClient
from spark_advisor_hs_connector.poller import HistoryServerPoller
from spark_advisor_hs_connector.polling_state import PollingState

settings = ConnectorSettings()
broker = NatsBroker(settings.nats.url)
app = FastStream(broker)

logger = logging.getLogger(__name__)


@app.on_startup
async def on_startup() -> None:
    logging.basicConfig(level=settings.log_level)

    hs_client = HistoryServerClient(settings.history_server_url, settings.history_server_timeout)
    hs_client.__enter__()

    polling_state = PollingState()
    poller = HistoryServerPoller(
        hs_client=hs_client,
        broker=broker,
        publish_subject=settings.nats.publish_subject,
        polling_state=polling_state,
        batch_size=settings.batch_size,
    )

    app.context.set_global("poller", poller)
    app.context.set_global("hs_client", hs_client)

    logger.info(
        "HS Connector started: history_server=%s poll_interval=%ds",
        settings.history_server_url,
        settings.poll_interval_seconds,
    )


_polling_task: asyncio.Task[None] | None = None


@app.after_startup
async def start_polling() -> None:
    global _polling_task
    poller: HistoryServerPoller = app.context.get("poller")
    _polling_task = asyncio.create_task(_polling_loop(poller, settings.poll_interval_seconds))


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
