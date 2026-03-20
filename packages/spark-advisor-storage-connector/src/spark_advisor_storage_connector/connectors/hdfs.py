from __future__ import annotations

from datetime import UTC, datetime

import httpx

from spark_advisor_storage_connector.config import HdfsSettings  # noqa: TC001
from spark_advisor_storage_connector.connectors.protocol import EventLogRef


class HdfsConnector:
    def __init__(self, settings: HdfsSettings) -> None:
        self._base_url = settings.namenode_url.rstrip("/")
        self._event_log_dir = settings.event_log_dir
        self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

    async def list_event_logs(self) -> list[EventLogRef]:
        url = f"{self._base_url}/webhdfs/v1{self._event_log_dir}?op=LISTSTATUS"
        response = await self._client.get(url)
        response.raise_for_status()
        data = response.json()

        refs: list[EventLogRef] = []
        for entry in data.get("FileStatuses", {}).get("FileStatus", []):
            if entry.get("type") != "FILE":
                continue
            name = entry["pathSuffix"]
            refs.append(
                EventLogRef(
                    path=f"{self._event_log_dir}/{name}",
                    name=name,
                    size=entry.get("length", 0),
                    modified_at=datetime.fromtimestamp(entry.get("modificationTime", 0) / 1000, tz=UTC),
                )
            )
        return refs

    async def read_event_log(self, ref: EventLogRef) -> bytes:
        url = f"{self._base_url}/webhdfs/v1{ref.path}?op=OPEN"
        response = await self._client.get(url)
        response.raise_for_status()
        return response.content

    async def close(self) -> None:
        await self._client.aclose()
