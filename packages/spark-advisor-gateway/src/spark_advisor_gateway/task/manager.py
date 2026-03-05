import logging
import uuid
from datetime import UTC, datetime

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.model import AnalysisResult

logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, store: TaskStore) -> None:
        self._store = store

    async def create(self, app_id: str) -> AnalysisTask:
        task = AnalysisTask(task_id=str(uuid.uuid4()), app_id=app_id)
        await self._store.create(task)
        return task

    async def get(self, task_id: str) -> AnalysisTask | None:
        return await self._store.get(task_id)

    async def list_recent(self, limit: int = 50) -> list[AnalysisTask]:
        return await self._store.list_recent(limit)

    async def mark_running(self, task_id: str) -> None:
        task = await self._store.get(task_id)
        if not task:
            logger.warning("Task %s not found for mark_running", task_id)
            return
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(UTC)
        await self._store.update(task)

    async def mark_completed(self, task_id: str, result: AnalysisResult) -> None:
        task = await self._store.get(task_id)
        if not task:
            logger.warning("Task %s not found for mark_completed", task_id)
            return
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now(UTC)
        task.result = result
        await self._store.update(task)

    async def mark_failed(self, task_id: str, error: str) -> None:
        task = await self._store.get(task_id)
        if not task:
            logger.warning("Task %s not found for mark_failed", task_id)
            return
        task.status = TaskStatus.FAILED
        task.completed_at = datetime.now(UTC)
        task.error = error
        await self._store.update(task)
