from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from starlette.testclient import TestClient

from spark_advisor_gateway.ws.manager import ConnectionManager
from spark_advisor_models.model import AnalysisResult
from spark_advisor_models.testing import make_job

if TYPE_CHECKING:
    from fastapi import FastAPI

    from spark_advisor_gateway.task.manager import TaskManager


@pytest.mark.asyncio
async def test_ws_connect_and_receive_update(app: FastAPI, task_manager: TaskManager) -> None:
    client = TestClient(app)
    with client.websocket_connect("/api/v1/ws/tasks") as ws:
        task = await task_manager.create("app-ws1")
        await task_manager.mark_running(task.task_id)
        msg = ws.receive_json()
        assert msg["event"] == "status"
        assert msg["data"]["task_id"] == task.task_id
        assert msg["data"]["status"] == "running"


@pytest.mark.asyncio
async def test_ws_subscribe_to_specific_task(app: FastAPI, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-ws2")
    client = TestClient(app)
    with client.websocket_connect(f"/api/v1/ws/tasks?task_ids={task.task_id}") as ws:
        await task_manager.mark_running(task.task_id)
        msg = ws.receive_json()
        assert msg["task_id"] == task.task_id
        assert msg["data"]["status"] == "running"


@pytest.mark.asyncio
async def test_ws_receives_only_subscribed_events(app: FastAPI, task_manager: TaskManager) -> None:
    task1 = await task_manager.create("app-ws3a")
    task2 = await task_manager.create("app-ws3b")
    client = TestClient(app)
    with client.websocket_connect(f"/api/v1/ws/tasks?task_ids={task1.task_id}") as ws:
        await task_manager.mark_running(task2.task_id)
        await task_manager.mark_running(task1.task_id)
        msg = ws.receive_json()
        assert msg["task_id"] == task1.task_id


@pytest.mark.asyncio
async def test_ws_global_receives_all_events(app: FastAPI, task_manager: TaskManager) -> None:
    client = TestClient(app)
    with client.websocket_connect("/api/v1/ws/tasks") as ws:
        t1 = await task_manager.create("app-ws4a")
        t2 = await task_manager.create("app-ws4b")
        await task_manager.mark_running(t1.task_id)
        await task_manager.mark_running(t2.task_id)
        msg1 = ws.receive_json()
        msg2 = ws.receive_json()
        received_ids = {msg1["task_id"], msg2["task_id"]}
        assert received_ids == {t1.task_id, t2.task_id}


@pytest.mark.asyncio
async def test_ws_completed_event_has_status(app: FastAPI, task_manager: TaskManager) -> None:
    client = TestClient(app)
    with client.websocket_connect("/api/v1/ws/tasks") as ws:
        task = await task_manager.create("app-ws5")
        await task_manager.mark_running(task.task_id)
        ws.receive_json()

        job = make_job(app_id="app-ws5")
        result = AnalysisResult(app_id="app-ws5", job=job, rule_results=[])
        await task_manager.mark_completed(task.task_id, result)
        msg = ws.receive_json()
        assert msg["data"]["status"] == "completed"


@pytest.mark.asyncio
async def test_ws_failed_event_has_error(app: FastAPI, task_manager: TaskManager) -> None:
    client = TestClient(app)
    with client.websocket_connect("/api/v1/ws/tasks") as ws:
        task = await task_manager.create("app-ws6")
        await task_manager.mark_running(task.task_id)
        ws.receive_json()

        await task_manager.mark_failed(task.task_id, "OOM killed")
        msg = ws.receive_json()
        assert msg["data"]["status"] == "failed"
        assert msg["data"]["error"] == "OOM killed"


def test_connection_manager_disconnect() -> None:
    manager = ConnectionManager()
    assert len(manager._global) == 0
    assert len(manager._subscriptions) == 0


@pytest.mark.asyncio
async def test_old_sse_endpoint_returns_404(app: FastAPI) -> None:
    from httpx import ASGITransport, AsyncClient

    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/v1/tasks/some-id/stream")
        assert response.status_code in (404, 405)
