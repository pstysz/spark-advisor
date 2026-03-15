from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from spark_advisor_models.model import (
    AdvisorReport,
    AnalysisMode,
    AnalysisResult,
    Recommendation,
    RuleResult,
    Severity,
)
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


def _make_rule_result(**overrides: object) -> RuleResult:
    defaults: dict[str, object] = {
        "rule_id": "shuffle_partitions",
        "severity": Severity.WARNING,
        "title": "Shuffle partitions too low",
        "message": "Consider increasing shuffle partitions",
        "current_value": "200",
        "recommended_value": "800",
        "estimated_impact": "~40% reduction in shuffle time",
    }
    defaults.update(overrides)
    return RuleResult(**defaults)  # type: ignore[arg-type]


async def _create_completed_task(
    task_manager: TaskManager,
    app_id: str = "app-hist",
    rule_results: list[RuleResult] | None = None,
    ai_report: AdvisorReport | None = None,
) -> str:
    task = await task_manager.create(app_id)
    await task_manager.mark_running(task.task_id)
    job = make_job(app_id=app_id)
    result = AnalysisResult(
        app_id=app_id,
        job=job,
        rule_results=rule_results or [],
        ai_report=ai_report,
    )
    await task_manager.mark_completed(task.task_id, result)
    return task.task_id


@pytest.mark.asyncio
async def test_app_history_returns_analyses(client: AsyncClient, task_manager: TaskManager) -> None:
    await _create_completed_task(task_manager, "app-h1")
    await _create_completed_task(task_manager, "app-h1")
    await _create_completed_task(task_manager, "app-other")

    response = await client.get("/api/v1/apps/app-h1/history")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["items"]) == 2
    assert all(item["app_id"] == "app-h1" for item in data["items"])


@pytest.mark.asyncio
async def test_app_history_empty_for_unknown(client: AsyncClient) -> None:
    response = await client.get("/api/v1/apps/unknown-app/history")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert len(data["items"]) == 0


@pytest.mark.asyncio
async def test_app_history_pagination(client: AsyncClient, task_manager: TaskManager) -> None:
    for _ in range(5):
        await _create_completed_task(task_manager, "app-pag")

    response = await client.get("/api/v1/apps/app-pag/history?limit=2&offset=1")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 5
    assert len(data["items"]) == 2
    assert data["limit"] == 2
    assert data["offset"] == 1


@pytest.mark.asyncio
async def test_task_rules_returns_violations(client: AsyncClient, task_manager: TaskManager) -> None:
    rules = [_make_rule_result(), _make_rule_result(rule_id="gc_pressure", severity=Severity.CRITICAL)]
    task_id = await _create_completed_task(task_manager, rule_results=rules)

    response = await client.get(f"/api/v1/tasks/{task_id}/rules")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["rule_id"] == "shuffle_partitions"
    assert data[1]["rule_id"] == "gc_pressure"
    assert data[1]["severity"] == "CRITICAL"


@pytest.mark.asyncio
async def test_task_rules_pending_returns_409(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-p")
    response = await client.get(f"/api/v1/tasks/{task.task_id}/rules")
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_task_rules_running_returns_409(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-r")
    await task_manager.mark_running(task.task_id)
    response = await client.get(f"/api/v1/tasks/{task.task_id}/rules")
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_task_rules_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/nonexistent/rules")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_task_rules_empty_when_no_violations(client: AsyncClient, task_manager: TaskManager) -> None:
    task_id = await _create_completed_task(task_manager, rule_results=[])
    response = await client.get(f"/api/v1/tasks/{task_id}/rules")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_task_config_returns_comparison(client: AsyncClient, task_manager: TaskManager) -> None:
    rules = [_make_rule_result(recommended_value="800")]
    ai_report = AdvisorReport(
        app_id="app-cfg",
        summary="Test",
        severity=Severity.WARNING,
        rule_results=[],
        recommendations=[
            Recommendation(parameter="spark.sql.shuffle.partitions", current_value="200", recommended_value="1000"),
        ],
        suggested_config={"spark.executor.memory": "8g"},
    )
    task_id = await _create_completed_task(task_manager, app_id="app-cfg", rule_results=rules, ai_report=ai_report)

    response = await client.get(f"/api/v1/tasks/{task_id}/config")
    assert response.status_code == 200
    data = response.json()
    assert data["app_id"] == "app-cfg"
    entries = {e["parameter"]: e for e in data["entries"]}
    assert "spark.executor.memory" in entries
    assert entries["spark.executor.memory"]["source"] == "ai"
    assert entries["spark.sql.shuffle.partitions"]["source"] == "ai"


@pytest.mark.asyncio
async def test_task_config_not_completed_returns_409(client: AsyncClient, task_manager: TaskManager) -> None:
    task = await task_manager.create("app-nc")
    response = await client.get(f"/api/v1/tasks/{task.task_id}/config")
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_task_config_not_found(client: AsyncClient) -> None:
    response = await client.get("/api/v1/tasks/nonexistent/config")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_task_config_without_ai_report(client: AsyncClient, task_manager: TaskManager) -> None:
    rules = [_make_rule_result(recommended_value="800")]
    task_id = await _create_completed_task(task_manager, rule_results=rules)

    response = await client.get(f"/api/v1/tasks/{task_id}/config")
    assert response.status_code == 200
    data = response.json()
    assert len(data["entries"]) >= 1
    assert all(e["source"] == "rule" for e in data["entries"])


@pytest.mark.asyncio
async def test_stats_summary_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/stats/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["completed"] == 0
    assert data["failed"] == 0
    assert data["avg_duration_seconds"] is None
    assert data["ai_usage_percent"] is None


@pytest.mark.asyncio
async def test_stats_summary_with_data(client: AsyncClient, task_manager: TaskManager) -> None:
    await _create_completed_task(task_manager, "app-s1", rule_results=[_make_rule_result()])
    await _create_completed_task(task_manager, "app-s2", rule_results=[])
    task_f = await task_manager.create("app-s3")
    await task_manager.mark_running(task_f.task_id)
    await task_manager.mark_failed(task_f.task_id, "timeout")

    response = await client.get("/api/v1/stats/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["completed"] == 2
    assert data["failed"] == 1
    assert data["avg_duration_seconds"] is not None
    assert data["ai_usage_percent"] == 0.0


@pytest.mark.asyncio
async def test_stats_summary_ai_usage(client: AsyncClient, task_manager: TaskManager) -> None:
    ai_report = AdvisorReport(
        app_id="app-ai",
        summary="AI analysis",
        severity=Severity.INFO,
        rule_results=[],
        recommendations=[],
    )
    await _create_completed_task(task_manager, "app-ai1", ai_report=ai_report)
    await _create_completed_task(task_manager, "app-ai2")

    response = await client.get("/api/v1/stats/summary")
    data = response.json()
    assert data["ai_usage_percent"] == 50.0


@pytest.mark.asyncio
async def test_stats_rules_counts_occurrences(client: AsyncClient, task_manager: TaskManager) -> None:
    r1 = _make_rule_result(rule_id="shuffle_partitions")
    r2 = _make_rule_result(rule_id="gc_pressure", severity=Severity.CRITICAL)
    await _create_completed_task(task_manager, "app-rf1", rule_results=[r1, r2])
    await _create_completed_task(task_manager, "app-rf2", rule_results=[r1])

    response = await client.get("/api/v1/stats/rules")
    assert response.status_code == 200
    data = response.json()
    items = {i["rule_id"]: i for i in data["items"]}
    assert items["shuffle_partitions"]["count"] == 2
    assert items["gc_pressure"]["count"] == 1
    assert data["days"] == 30


@pytest.mark.asyncio
async def test_stats_rules_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/stats/rules")
    assert response.status_code == 200
    assert response.json()["items"] == []


@pytest.mark.asyncio
async def test_stats_daily_volume(client: AsyncClient, task_manager: TaskManager) -> None:
    await _create_completed_task(task_manager, "app-dv1")
    await _create_completed_task(task_manager, "app-dv2")

    response = await client.get("/api/v1/stats/daily-volume")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) >= 1
    total_count = sum(i["count"] for i in data["items"])
    assert total_count >= 2


@pytest.mark.asyncio
async def test_stats_daily_volume_empty(client: AsyncClient) -> None:
    response = await client.get("/api/v1/stats/daily-volume")
    assert response.status_code == 200
    assert response.json()["items"] == []


@pytest.mark.asyncio
async def test_stats_top_issues(client: AsyncClient, task_manager: TaskManager) -> None:
    r1 = _make_rule_result(rule_id="shuffle_partitions")
    r2 = _make_rule_result(rule_id="gc_pressure", severity=Severity.CRITICAL)
    await _create_completed_task(task_manager, "app-ti1", rule_results=[r1, r2])
    await _create_completed_task(task_manager, "app-ti2", rule_results=[r1])

    response = await client.get("/api/v1/stats/top-issues?limit=1")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["rule_id"] == "shuffle_partitions"
    assert data["items"][0]["count"] == 2
    assert data["items"][0]["example_app_id"] in ("app-ti1", "app-ti2")
    assert data["limit"] == 1
