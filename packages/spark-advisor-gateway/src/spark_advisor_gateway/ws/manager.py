from __future__ import annotations

import asyncio
import contextlib
import logging

from starlette.websockets import WebSocket, WebSocketState

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, heartbeat_interval: float = 30.0) -> None:
        self._subscriptions: dict[str, set[WebSocket]] = {}
        self._global: set[WebSocket] = set()
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_task: asyncio.Task[None] | None = None

    async def connect(self, ws: WebSocket, task_ids: list[str] | None = None) -> None:
        await ws.accept()
        if task_ids:
            for tid in task_ids:
                self._subscriptions.setdefault(tid, set()).add(ws)
        else:
            self._global.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._global.discard(ws)
        empty_keys: list[str] = []
        for tid, subs in self._subscriptions.items():
            subs.discard(ws)
            if not subs:
                empty_keys.append(tid)
        for key in empty_keys:
            del self._subscriptions[key]

    async def broadcast(self, task_id: str, data: dict[str, object]) -> None:
        event = {"event": "status", "task_id": task_id, "data": data}
        targets = self._subscriptions.get(task_id, set()) | self._global
        stale: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(event)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.disconnect(ws)

    async def start(self) -> None:
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            stale: list[WebSocket] = []
            all_ws = self._global | {ws for subs in self._subscriptions.values() for ws in subs}
            for ws in all_ws:
                try:
                    if ws.client_state == WebSocketState.CONNECTED:
                        await ws.send_json({"event": "ping"})
                except Exception:
                    stale.append(ws)
            for ws in stale:
                self.disconnect(ws)
