import asyncio
from typing import Protocol

import structlog

logger = structlog.stdlib.get_logger(__name__)


class Poller(Protocol):
    async def poll(self) -> int: ...


async def polling_loop(poller: Poller, interval: int) -> None:
    while True:
        try:
            count = await poller.poll()
            if count > 0:
                logger.info("Poll cycle: %d new items published", count)
        except Exception:
            logger.exception("Poll cycle failed")
        await asyncio.sleep(interval)
