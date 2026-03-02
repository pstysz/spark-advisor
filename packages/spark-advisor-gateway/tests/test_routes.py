from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

if TYPE_CHECKING:
    from httpx import AsyncClient

    from spark_advisor_gateway.task.manager import TaskManager


@pytest.mark.asyncio
async def test_analyze_returns_202(client: AsyncClient) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": "app-123"})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert "task_id" in data


@pytest.mark.asyncio
async def test_get_task_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_task_returns_pending(client: AsyncClient, task_manager: TaskManager) -> None:
    task = task_manager.create("app-456")
    response = await client.get(f"/api/v1/tasks/{task.task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["app_id"] == "app-456"
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_list_tasks_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_list_tasks_returns_created(client: AsyncClient, task_manager: TaskManager) -> None:
    task_manager.create("app-a")
    task_manager.create("app-b")
    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2


@pytest.mark.asyncio
async def test_list_tasks_respects_limit(client: AsyncClient, task_manager: TaskManager) -> None:
    for i in range(5):
        task_manager.create(f"app-{i}")
    response = await client.get("/api/v1/tasks?limit=2")
    assert response.status_code == 200
    assert len(response.json()) == 2


def _nats_reply(data: object) -> MagicMock:
    msg = MagicMock()
    msg.data = orjson.dumps(data)
    return msg


@pytest.mark.asyncio
async def test_list_applications_returns_apps(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply([
        {
            "id": "app-001",
            "name": "SparkPi",
            "attempts": [
                {
                    "startTime": "2026-02-26T18:06:24.459GMT",
                    "endTime": "2026-02-26T18:07:58.746GMT",
                    "duration": 5000,
                    "completed": True,
                    "appSparkVersion": "3.5.0",
                    "sparkUser": "hdfs",
                }
            ],
        },
        {"id": "app-002", "name": "ETL Job", "attempts": []},
    ])
    response = await client.get("/api/v1/applications")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["id"] == "app-001"
    assert data[0]["name"] == "SparkPi"
    assert data[0]["duration_ms"] == 5000
    assert data[0]["completed"] is True
    assert data[0]["spark_version"] == "3.5.0"
    assert data[0]["user"] == "hdfs"
    assert data[0]["start_time"] == "2026-02-26T18:06:24.459GMT"
    assert data[0]["end_time"] == "2026-02-26T18:07:58.746GMT"
    assert data[1]["id"] == "app-002"
    assert data[1]["duration_ms"] == 0


@pytest.mark.asyncio
async def test_list_applications_passes_limit(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply([])
    response = await client.get("/api/v1/applications?limit=5")
    assert response.status_code == 200
    call_args = mock_nc.request.call_args
    payload = orjson.loads(call_args[0][1])
    assert payload["limit"] == 5


@pytest.mark.asyncio
async def test_list_applications_returns_502_on_error(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply({"error": "Connection refused"})
    response = await client.get("/api/v1/applications")
    assert response.status_code == 502
    assert "Connection refused" in response.json()["detail"]
