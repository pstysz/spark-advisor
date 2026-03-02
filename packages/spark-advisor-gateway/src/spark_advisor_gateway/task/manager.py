import uuid
from datetime import UTC, datetime

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.model import AnalysisResult


class TaskManager:
    def __init__(self, store: TaskStore) -> None:
        self._store = store

    def create(self, app_id: str) -> AnalysisTask:
        task = AnalysisTask(task_id=str(uuid.uuid4()), app_id=app_id)
        self._store.create(task)
        return task

    def get(self, task_id: str) -> AnalysisTask | None:
        return self._store.get(task_id)

    def list_recent(self, limit: int = 50) -> list[AnalysisTask]:
        return self._store.list_recent(limit)

    def mark_running(self, task_id: str) -> None:
        task = self._store.get(task_id)
        if task:
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(UTC)
            self._store.update(task)

    def mark_completed(self, task_id: str, result: AnalysisResult) -> None:
        task = self._store.get(task_id)
        if task:
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(UTC)
            task.result = result
            self._store.update(task)

    def mark_failed(self, task_id: str, error: str) -> None:
        task = self._store.get(task_id)
        if task:
            task.status = TaskStatus.FAILED
            task.completed_at = datetime.now(UTC)
            task.error = error
            self._store.update(task)
