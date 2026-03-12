from __future__ import annotations

import pytest

from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.models import TaskStatus
from spark_advisor_gateway.task.store import SqlAlchemyTaskStore
from spark_advisor_models.model import AnalysisResult
from spark_advisor_models.testing import make_job


async def _make_manager() -> TaskManager:
    store = SqlAlchemyTaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()
    return TaskManager(store)


class TestTaskManagerCreate:
    @pytest.mark.asyncio
    async def test_creates_task_with_pending_status(self) -> None:
        manager = await _make_manager()
        task = await manager.create("app-1")
        assert task.status == TaskStatus.PENDING
        assert task.app_id == "app-1"
        assert task.task_id

    @pytest.mark.asyncio
    async def test_creates_unique_ids(self) -> None:
        manager = await _make_manager()
        t1 = await manager.create("app-1")
        t2 = await manager.create("app-2")
        assert t1.task_id != t2.task_id


class TestTaskManagerGet:
    @pytest.mark.asyncio
    async def test_returns_existing_task(self) -> None:
        manager = await _make_manager()
        task = await manager.create("app-1")
        found = await manager.get(task.task_id)
        assert found is not None
        assert found.app_id == "app-1"

    @pytest.mark.asyncio
    async def test_returns_none_for_unknown(self) -> None:
        manager = await _make_manager()
        assert await manager.get("nonexistent") is None


class TestTaskManagerListRecent:
    @pytest.mark.asyncio
    async def test_returns_empty_when_no_tasks(self) -> None:
        manager = await _make_manager()
        assert await manager.list_recent() == []

    @pytest.mark.asyncio
    async def test_returns_tasks_in_reverse_order(self) -> None:
        manager = await _make_manager()
        await manager.create("first")
        await manager.create("second")
        tasks = await manager.list_recent()
        assert tasks[0].app_id == "second"
        assert tasks[1].app_id == "first"

    @pytest.mark.asyncio
    async def test_respects_limit(self) -> None:
        manager = await _make_manager()
        for i in range(10):
            await manager.create(f"app-{i}")
        assert len(await manager.list_recent(3)) == 3


class TestTaskManagerMarkRunning:
    @pytest.mark.asyncio
    async def test_marks_running(self) -> None:
        manager = await _make_manager()
        task = await manager.create("app-1")
        await manager.mark_running(task.task_id)
        updated = await manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.RUNNING
        assert updated.started_at is not None


class TestTaskManagerMarkCompleted:
    @pytest.mark.asyncio
    async def test_marks_completed_with_result(self) -> None:
        manager = await _make_manager()
        task = await manager.create("app-1")
        job = make_job()
        result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)
        await manager.mark_completed(task.task_id, result)
        updated = await manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.COMPLETED
        assert updated.completed_at is not None
        assert updated.result is not None


class TestTaskManagerMarkFailed:
    @pytest.mark.asyncio
    async def test_marks_failed_with_error(self) -> None:
        manager = await _make_manager()
        task = await manager.create("app-1")
        await manager.mark_failed(task.task_id, "connection timeout")
        updated = await manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.FAILED
        assert updated.error == "connection timeout"
        assert updated.completed_at is not None


class TestTaskManagerListFiltered:
    @pytest.mark.asyncio
    async def test_filters_by_status(self) -> None:
        manager = await _make_manager()
        t1 = await manager.create("app-a")
        await manager.create("app-b")
        await manager.mark_running(t1.task_id)
        tasks, total = await manager.list_filtered(status=TaskStatus.RUNNING)
        assert total == 1
        assert tasks[0].status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_filters_by_app_id(self) -> None:
        manager = await _make_manager()
        await manager.create("app-a")
        await manager.create("app-b")
        await manager.create("app-a")
        _tasks, total = await manager.list_filtered(app_id="app-a")
        assert total == 2

    @pytest.mark.asyncio
    async def test_pagination(self) -> None:
        manager = await _make_manager()
        for i in range(5):
            await manager.create(f"app-{i}")
        tasks, total = await manager.list_filtered(limit=2, offset=1)
        assert total == 5
        assert len(tasks) == 2


class TestTaskManagerCountByStatus:
    @pytest.mark.asyncio
    async def test_returns_counts(self) -> None:
        manager = await _make_manager()
        t1 = await manager.create("app-a")
        await manager.create("app-b")
        await manager.mark_running(t1.task_id)
        counts = await manager.count_by_status()
        assert counts[TaskStatus.RUNNING] == 1
        assert counts[TaskStatus.PENDING] == 1

    @pytest.mark.asyncio
    async def test_empty_store(self) -> None:
        manager = await _make_manager()
        counts = await manager.count_by_status()
        assert counts == {}
