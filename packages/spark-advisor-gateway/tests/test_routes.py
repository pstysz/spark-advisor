from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from spark_advisor_models.model import AnalysisMode, AnalysisResult
from spark_advisor_models.testing import make_job

if TYPE_CHECKING:
    from httpx import AsyncClient

    from spark_advisor_gateway.task.executor import TaskExecutor
    from spark_advisor_gateway.task.manager import TaskManager


@pytest.mark.asyncio
async def test_analyze_returns_202(client: AsyncClient) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": "app-123"})
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert "task_id" in data


@pytest.mark.asyncio
async def test_analyze_with_agent_mode(client: AsyncClient, task_executor: TaskExecutor) -> None:
    with patch.object(task_executor, "submit") as mock_submit:
        response = await client.post("/api/v1/analyze", json={"app_id": "app-123", "mode": "agent"})
    assert response.status_code == 202
    call_args = mock_submit.call_args
    assert call_args.args[2] == AnalysisMode.AGENT


@pytest.mark.asyncio
async def test_analyze_default_mode_is_standard(client: AsyncClient, task_executor: TaskExecutor) -> None:
    with patch.object(task_executor, "submit") as mock_submit:
        response = await client.post("/api/v1/analyze", json={"app_id": "app-123"})
    assert response.status_code == 202
    call_args = mock_submit.call_args
    assert call_args.args[2] == AnalysisMode.AI


@pytest.mark.asyncio
async def test_get_task_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/nonexistent")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_task_returns_pending(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-456")
    response = await client.get(f"/api/v1/tasks/{task.task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["app_id"] == "app-456"
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_list_tasks_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    data = response.json()
    assert data["items"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_list_tasks_returns_created(client: AsyncClient, task_manager: TaskManager) -> None:
    await task_manager.create("app-a")
    await task_manager.create("app-b")
    response = await client.get("/api/v1/tasks")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] == 2


@pytest.mark.asyncio
async def test_list_tasks_respects_limit(client: AsyncClient, task_manager: TaskManager) -> None:
    for i in range(5):
        await task_manager.create(f"app-{i}")
    response = await client.get("/api/v1/tasks?limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] == 5
    assert data["limit"] == 2
    assert data["offset"] == 0


@pytest.mark.asyncio
async def test_list_tasks_with_offset(client: AsyncClient, task_manager: TaskManager) -> None:
    for i in range(5):
        await task_manager.create(f"app-{i}")
    response = await client.get("/api/v1/tasks?limit=2&offset=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] == 5
    assert data["offset"] == 2


@pytest.mark.asyncio
async def test_list_tasks_filter_by_status(client: AsyncClient, task_manager: TaskManager) -> None:
    t1 = await task_manager.create("app-a")
    await task_manager.create("app-b")
    await task_manager.mark_running(t1.task_id)
    response = await client.get("/api/v1/tasks?status=running")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["status"] == "running"
    assert data["total"] == 1


@pytest.mark.asyncio
async def test_list_tasks_invalid_status_returns_422(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks?status=invalid")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_filter_by_app_id(client: AsyncClient, task_manager: TaskManager) -> None:
    await task_manager.create("app-a")
    await task_manager.create("app-b")
    await task_manager.create("app-a")
    response = await client.get("/api/v1/tasks?app_id=app-a")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 2
    assert data["total"] == 2


@pytest.mark.asyncio
async def test_task_stats_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["counts"] == {}
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_task_stats_with_tasks(client: AsyncClient, task_manager: TaskManager) -> None:
    t1 = await task_manager.create("app-a")
    await task_manager.create("app-b")
    await task_manager.mark_running(t1.task_id)
    response = await client.get("/api/v1/tasks/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["counts"]["running"] == 1
    assert data["counts"]["pending"] == 1
    assert data["total"] == 2


def _nats_reply(data: object) -> MagicMock:
    msg = MagicMock()
    msg.data = orjson.dumps(data)
    return msg


@pytest.mark.asyncio
async def test_list_applications_returns_apps(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply(
        [
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
        ]
    )
    response = await client.get("/api/v1/applications")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert data["limit"] == 20
    assert data["offset"] == 0
    items = data["items"]
    assert len(items) == 2
    assert items[0]["id"] == "app-001"
    assert items[0]["name"] == "SparkPi"
    assert items[0]["duration_ms"] == 5000
    assert items[0]["completed"] is True
    assert items[0]["spark_version"] == "3.5.0"
    assert items[0]["user"] == "hdfs"
    assert items[0]["start_time"] == "2026-02-26T18:06:24.459GMT"
    assert items[0]["end_time"] == "2026-02-26T18:07:58.746GMT"
    assert items[1]["id"] == "app-002"
    assert items[1]["duration_ms"] == 0


@pytest.mark.asyncio
async def test_list_applications_passes_limit(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply([])
    response = await client.get("/api/v1/applications?limit=5")
    assert response.status_code == 200
    call_args = mock_nc.request.call_args
    payload = orjson.loads(call_args[0][1])
    assert payload["limit"] == 5
    data = response.json()
    assert data["limit"] == 5
    assert data["offset"] == 0


@pytest.mark.asyncio
async def test_list_applications_with_offset(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply(
        [
            {"id": f"app-{i:03d}", "name": f"Job {i}", "attempts": []}
            for i in range(5)
        ]
    )
    response = await client.get("/api/v1/applications?limit=2&offset=2")
    assert response.status_code == 200
    data = response.json()
    assert data["offset"] == 2
    assert data["limit"] == 2
    assert data["total"] == 5
    assert len(data["items"]) == 2
    assert data["items"][0]["id"] == "app-002"
    assert data["items"][1]["id"] == "app-003"
    call_args = mock_nc.request.call_args
    payload = orjson.loads(call_args[0][1])
    assert payload["limit"] == 4


@pytest.mark.asyncio
async def test_list_applications_offset_beyond_total(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply(
        [{"id": "app-001", "name": "Job 1", "attempts": []}]
    )
    response = await client.get("/api/v1/applications?limit=10&offset=5")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["items"]) == 0
    assert data["offset"] == 5


@pytest.mark.asyncio
async def test_list_applications_returns_502_on_error(client: AsyncClient, mock_nc: AsyncMock) -> None:
    mock_nc.request.return_value = _nats_reply({"error": "Connection refused"})
    response = await client.get("/api/v1/applications")
    assert response.status_code == 502
    assert "Connection refused" in response.json()["detail"]


def _parse_sse_events(body: str) -> list[dict[str, str]]:
    events: list[dict[str, str]] = []
    for block in body.strip().split("\n\n"):
        event: dict[str, str] = {}
        for line in block.strip().split("\n"):
            if line.startswith("event: "):
                event["event"] = line[7:]
            elif line.startswith("data: "):
                event["data"] = line[6:]
        if event:
            events.append(event)
    return events


@pytest.mark.asyncio
async def test_stream_task_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/nonexistent/stream")
    assert response.status_code == 200
    events = _parse_sse_events(response.text)
    assert len(events) == 1
    assert events[0]["event"] == "error"
    assert "not found" in events[0]["data"].lower()


@pytest.mark.asyncio
async def test_stream_task_completed(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-123")
    job = make_job(app_id="app-123")
    result = AnalysisResult(app_id="app-123", job=job, rule_results=[])
    await task_manager.mark_completed(task.task_id, result)

    response = await client.get(f"/api/v1/tasks/{task.task_id}/stream")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse_events(response.text)
    assert len(events) == 1
    assert events[0]["event"] == "status"
    data = orjson.loads(events[0]["data"])
    assert data["status"] == "completed"


@pytest.mark.asyncio
async def test_stream_task_failed(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-123")
    await task_manager.mark_failed(task.task_id, "OOM killed")

    response = await client.get(f"/api/v1/tasks/{task.task_id}/stream")
    events = _parse_sse_events(response.text)
    assert len(events) == 1
    assert events[0]["event"] == "status"
    data = orjson.loads(events[0]["data"])
    assert data["status"] == "failed"
    assert data["error"] == "OOM killed"


@pytest.mark.asyncio
async def test_stream_task_transitions(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-123")
    await task_manager.mark_running(task.task_id)

    job = make_job(app_id="app-123")
    result = AnalysisResult(app_id="app-123", job=job, rule_results=[])
    await task_manager.mark_completed(task.task_id, result)

    response = await client.get(f"/api/v1/tasks/{task.task_id}/stream")
    events = _parse_sse_events(response.text)
    assert len(events) == 1
    assert events[0]["event"] == "status"
    data = orjson.loads(events[0]["data"])
    assert data["status"] == "completed"


@pytest.mark.asyncio
async def test_analyze_duplicate_pending_returns_409(client: AsyncClient) -> None:
    await client.post("/api/v1/analyze", json={"app_id": "app-dup"})
    response = await client.post("/api/v1/analyze", json={"app_id": "app-dup"})
    assert response.status_code == 409
    data = response.json()
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_analyze_duplicate_running_returns_409(client: AsyncClient, task_manager: TaskManager) -> None:
    r1 = await client.post("/api/v1/analyze", json={"app_id": "app-run"})
    task_id = r1.json()["task_id"]
    await task_manager.mark_running(task_id)

    response = await client.post("/api/v1/analyze", json={"app_id": "app-run"})
    assert response.status_code == 409
    assert response.json()["status"] == "running"


@pytest.mark.asyncio
async def test_analyze_after_completed_creates_new(
    client: AsyncClient, task_manager: TaskManager, task_executor: TaskExecutor
) -> None:
    with patch.object(task_executor, "submit"):
        r1 = await client.post("/api/v1/analyze", json={"app_id": "app-done"})
    task_id = r1.json()["task_id"]
    job = make_job(app_id="app-done")
    result = AnalysisResult(app_id="app-done", job=job, rule_results=[])
    await task_manager.mark_completed(task_id, result)

    response = await client.post("/api/v1/analyze", json={"app_id": "app-done"})
    assert response.status_code == 202
    assert response.json()["task_id"] != task_id


@pytest.mark.asyncio
async def test_analyze_rerun_completed_creates_new(
    client: AsyncClient, task_manager: TaskManager, task_executor: TaskExecutor
) -> None:
    with patch.object(task_executor, "submit"):
        r1 = await client.post("/api/v1/analyze", json={"app_id": "app-rerun"})
    task_id = r1.json()["task_id"]
    job = make_job(app_id="app-rerun")
    result = AnalysisResult(app_id="app-rerun", job=job, rule_results=[])
    await task_manager.mark_completed(task_id, result)

    response = await client.post("/api/v1/analyze", json={"app_id": "app-rerun", "rerun": True})
    assert response.status_code == 202
    assert response.json()["task_id"] != task_id


@pytest.mark.asyncio
async def test_analyze_rerun_running_returns_409(client: AsyncClient, task_manager: TaskManager) -> None:
    r1 = await client.post("/api/v1/analyze", json={"app_id": "app-busy"})
    task_id = r1.json()["task_id"]
    await task_manager.mark_running(task_id)

    response = await client.post("/api/v1/analyze", json={"app_id": "app-busy", "rerun": True})
    assert response.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("app_id", ["app-123", "application_1234567890123_0001", "my_app-v2"])
async def test_analyze_valid_app_id_formats(client: AsyncClient, app_id: str) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": app_id})
    assert response.status_code == 202


@pytest.mark.asyncio
async def test_analyze_app_id_too_long_returns_422(client: AsyncClient) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": "a" * 129})
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("app_id", ["../etc/passwd", "app/../secret", "app/../../root"])
async def test_analyze_app_id_traversal_returns_422(client: AsyncClient, app_id: str) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": app_id})
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("app_id", ["app id", "app@host", "app;rm -rf", "app&cmd", "app<script>"])
async def test_analyze_app_id_special_chars_returns_422(client: AsyncClient, app_id: str) -> None:
    response = await client.post("/api/v1/analyze", json={"app_id": app_id})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_negative_offset_returns_422(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks?offset=-1")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_limit_exceeds_max_returns_422(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks?limit=501")
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_tasks_limit_zero_returns_422(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks?limit=0")
    assert response.status_code == 422
