from __future__ import annotations

from typing import TYPE_CHECKING

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
