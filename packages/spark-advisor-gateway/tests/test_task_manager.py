from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "spark-advisor-models" / "tests"))

from factories import make_job

from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.models import TaskStatus
from spark_advisor_gateway.task.store import InMemoryTaskStore
from spark_advisor_models.model import AnalysisResult


def _make_manager() -> TaskManager:
    return TaskManager(InMemoryTaskStore())


class TestTaskManagerCreate:
    def test_creates_task_with_pending_status(self) -> None:
        manager = _make_manager()
        task = manager.create("app-1")
        assert task.status == TaskStatus.PENDING
        assert task.app_id == "app-1"
        assert task.task_id

    def test_creates_unique_ids(self) -> None:
        manager = _make_manager()
        t1 = manager.create("app-1")
        t2 = manager.create("app-2")
        assert t1.task_id != t2.task_id


class TestTaskManagerGet:
    def test_returns_existing_task(self) -> None:
        manager = _make_manager()
        task = manager.create("app-1")
        found = manager.get(task.task_id)
        assert found is not None
        assert found.app_id == "app-1"

    def test_returns_none_for_unknown(self) -> None:
        manager = _make_manager()
        assert manager.get("nonexistent") is None


class TestTaskManagerListRecent:
    def test_returns_empty_when_no_tasks(self) -> None:
        manager = _make_manager()
        assert manager.list_recent() == []

    def test_returns_tasks_in_reverse_order(self) -> None:
        manager = _make_manager()
        manager.create("first")
        manager.create("second")
        tasks = manager.list_recent()
        assert tasks[0].app_id == "second"
        assert tasks[1].app_id == "first"

    def test_respects_limit(self) -> None:
        manager = _make_manager()
        for i in range(10):
            manager.create(f"app-{i}")
        assert len(manager.list_recent(3)) == 3


class TestTaskManagerMarkRunning:
    def test_marks_running(self) -> None:
        manager = _make_manager()
        task = manager.create("app-1")
        manager.mark_running(task.task_id)
        updated = manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.RUNNING
        assert updated.started_at is not None


class TestTaskManagerMarkCompleted:
    def test_marks_completed_with_result(self) -> None:
        manager = _make_manager()
        task = manager.create("app-1")
        job = make_job()
        result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)
        manager.mark_completed(task.task_id, result)
        updated = manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.COMPLETED
        assert updated.completed_at is not None
        assert updated.result is not None


class TestTaskManagerMarkFailed:
    def test_marks_failed_with_error(self) -> None:
        manager = _make_manager()
        task = manager.create("app-1")
        manager.mark_failed(task.task_id, "connection timeout")
        updated = manager.get(task.task_id)
        assert updated is not None
        assert updated.status == TaskStatus.FAILED
        assert updated.error == "connection timeout"
        assert updated.completed_at is not None
