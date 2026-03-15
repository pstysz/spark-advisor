import logging
import uuid
from datetime import UTC, datetime

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.model import AnalysisResult

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED})


class TaskManager:
    def __init__(self, store: TaskStore) -> None:
        self._store = store

    async def create(self, app_id: str) -> AnalysisTask:
        task = AnalysisTask(task_id=str(uuid.uuid4()), app_id=app_id, created_at=datetime.now(UTC))
        await self._store.create(task)
        return task

    async def create_if_not_active(
        self, app_id: str, *, rerun: bool = False,
    ) -> tuple[AnalysisTask, bool] | None:
        """Try to create a task for app_id with deduplication.

        Returns:
            (task, True)  — new task created, proceed with analysis
            (task, False) — existing active task found, skip (dedup)
            None          — rerun requested but task still active, cannot proceed
        """
        existing = await self._store.find_latest_by_app_id(app_id)
        if existing is None:
            return await self.create(app_id), True

        if not rerun:
            if existing.status not in _TERMINAL_STATUSES:
                logger.info("Skipping %s — active task %s (%s)", app_id, existing.task_id, existing.status)
                return existing, False
            return await self.create(app_id), True

        if existing.status not in _TERMINAL_STATUSES:
            logger.info("Cannot rerun %s — task %s still %s", app_id, existing.task_id, existing.status)
            return None

        return await self.create(app_id), True

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

