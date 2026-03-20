from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime  # noqa: TC003
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class EventLogRef:
    path: str
    name: str
    size: int
    modified_at: datetime


@runtime_checkable
class StorageConnector(Protocol):
    async def list_event_logs(self) -> list[EventLogRef]: ...

    async def read_event_log(self, ref: EventLogRef) -> bytes: ...

    async def close(self) -> None: ...
