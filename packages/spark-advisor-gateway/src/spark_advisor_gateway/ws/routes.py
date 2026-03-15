from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from spark_advisor_gateway.config import StateKey

if TYPE_CHECKING:
    from spark_advisor_gateway.ws.manager import ConnectionManager

router = APIRouter()


@router.websocket("/api/v1/ws/tasks")
async def ws_tasks(websocket: WebSocket, task_ids: str | None = None) -> None:
    manager: ConnectionManager = getattr(websocket.app.state, StateKey.CONNECTION_MANAGER)
    ids = [tid.strip() for tid in task_ids.split(",") if tid.strip()] if task_ids else None
    await manager.connect(websocket, ids)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
