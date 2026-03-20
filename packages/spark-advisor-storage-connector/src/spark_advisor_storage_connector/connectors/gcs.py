from __future__ import annotations

from datetime import UTC, datetime

from gcloud.aio.storage import Storage

from spark_advisor_storage_connector.config import GcsSettings  # noqa: TC001
from spark_advisor_storage_connector.connectors.protocol import EventLogRef


class GcsConnector:
    def __init__(self, settings: GcsSettings) -> None:
        self._bucket = settings.bucket
        self._prefix = settings.prefix
        self._storage: Storage | None = None

    def _ensure_client(self) -> Storage:
        if self._storage is None:
            self._storage = Storage()
        return self._storage

    async def list_event_logs(self) -> list[EventLogRef]:
        storage = self._ensure_client()
        params = {"prefix": self._prefix}
        result = await storage.list_objects(self._bucket, params=params)

        refs: list[EventLogRef] = []
        for item in result.get("items", []):
            name: str = item["name"]
            filename = name.rsplit("/", 1)[-1]
            if not filename:
                continue
            updated = item.get("updated", "")
            modified_at = datetime.fromisoformat(updated.replace("Z", "+00:00")) if updated else datetime.now(UTC)
            refs.append(
                EventLogRef(
                    path=name,
                    name=filename,
                    size=int(item.get("size", 0)),
                    modified_at=modified_at,
                )
            )
        return refs

    async def read_event_log(self, ref: EventLogRef) -> bytes:
        return await self._ensure_client().download(self._bucket, ref.path)

    async def close(self) -> None:
        if self._storage is not None:
            await self._storage.close()
            self._storage = None
