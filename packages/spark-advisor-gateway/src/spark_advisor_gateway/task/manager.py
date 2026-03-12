import uuid
from datetime import UTC, datetime

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_gateway.task.store import SqlAlchemyTaskStore
from spark_advisor_models.model import AnalysisResult


class TaskManager:
    def __init__(self, store: SqlAlchemyTaskStore) -> None:
        self._store = store

    async def create(self, app_id: str) -> AnalysisTask:
        task = AnalysisTask(task_id=str(uuid.uuid4()), app_id=app_id, created_at=datetime.now(UTC))
        await self._store.create(task)
        return task

    async def get(self, task_id: str) -> AnalysisTask | None:
        return await self._store.get(task_id)

    async def list_recent(self, limit: int = 50) -> list[AnalysisTask]:
        return await self._store.list_recent(limit)

    async def list_filtered(
        self,
        *,
        limit: int | None = 50,
        offset: int = 0,
        status: TaskStatus | None = None,
        app_id: str | None = None,
    ) -> tuple[list[AnalysisTask], int]:
        return await self._store.list_filtered(limit=limit, offset=offset, status=status, app_id=app_id)

    async def count_by_status(self) -> dict[TaskStatus, int]:
        return await self._store.count_by_status()

    async def mark_running(self, task_id: str) -> None:
        task = await self._store.get(task_id)
        if task is None:
            return
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)
        await self._store.update(task)

    async def mark_completed(self, task_id: str, result: AnalysisResult) -> None:
        task = await self._store.get(task_id)
        if task is None:
            return
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now(UTC)
        task.result = result
        await self._store.update(task)

    async def mark_failed(self, task_id: str, error: str) -> None:
        task = await self._store.get(task_id)
        if task is None:
            return
        task.status = TaskStatus.FAILED
        task.completed_at = datetime.now(UTC)
        task.error = error
        await self._store.update(task)

