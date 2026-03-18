"""NATS connectivity health check for Kubernetes exec probes."""

from __future__ import annotations

import asyncio
import sys

import nats

from spark_advisor_hs_connector.config import ConnectorSettings


async def _check() -> bool:
    settings = ConnectorSettings()
    try:
        nc = await nats.connect(settings.nats.url, connect_timeout=3)
        await nc.drain()
        return True
    except Exception:
        return False


def main() -> None:
    ok = asyncio.run(_check())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
