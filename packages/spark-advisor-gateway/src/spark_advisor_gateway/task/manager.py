from __future__ import annotations

import logging
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from spark_advisor_gateway.api.schemas import (
        RuleFrequencyEntry,
        StatsSummaryResponse,
        TopIssueEntry,
    )
    from spark_advisor_gateway.task.store import TaskStore
    from spark_advisor_models.model import AnalysisResult, Severity

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED})


class TaskManager:
    def __init__(
        self,
        store: TaskStore,
        on_status_change: Callable[[str, dict[str, object]], Awaitable[None]] | None = None,
    ) -> None:
        self._store = store
        self._on_status_change = on_status_change

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
        await self._notify(task)

    async def mark_completed(self, task_id: str, result: AnalysisResult) -> None:
        task = await self._store.get(task_id)
        if task is None:
            return
        task.status = TaskStatus.COMPLETED
        task.completed_at = datetime.now(UTC)
        task.result = result
        await self._store.update(task)
        await self._notify(task)

    async def mark_failed(self, task_id: str, error: str) -> None:
        task = await self._store.get(task_id)
        if task is None:
            return
        task.status = TaskStatus.FAILED
        task.completed_at = datetime.now(UTC)
        task.error = error
        await self._store.update(task)
        await self._notify(task)

    async def _notify(self, task: AnalysisTask) -> None:
        if self._on_status_change is None:
            return
        data: dict[str, object] = {
            "task_id": task.task_id,
            "app_id": task.app_id,
            "status": task.status.value,
        }
        if task.started_at:
            data["started_at"] = task.started_at.isoformat()
        if task.completed_at:
            data["completed_at"] = task.completed_at.isoformat()
        if task.error:
            data["error"] = task.error
        await self._on_status_change(task.task_id, data)

    async def get_stats_summary(self, days: int = 30) -> StatsSummaryResponse:
        from spark_advisor_gateway.api.schemas import StatsSummaryResponse

        since = datetime.now(UTC) - timedelta(days=days)
        counts = await self._store.count_since(since)
        total = sum(counts.values())
        completed = counts.get(TaskStatus.COMPLETED, 0)
        failed = counts.get(TaskStatus.FAILED, 0)

        avg_duration: float | None = None
        ai_usage_pct: float | None = None
        if completed > 0:
            completed_tasks = await self._store.list_completed_since(since)
            durations = [
                (t.completed_at - t.started_at).total_seconds()
                for t in completed_tasks
                if t.started_at and t.completed_at
            ]
            avg_duration = sum(durations) / len(durations) if durations else None
            ai_count = sum(1 for t in completed_tasks if t.result and t.result.ai_report)
            ai_usage_pct = (ai_count / completed) * 100

        return StatsSummaryResponse(
            total=total,
            completed=completed,
            failed=failed,
            avg_duration_seconds=round(avg_duration, 2) if avg_duration is not None else None,
            ai_usage_percent=round(ai_usage_pct, 1) if ai_usage_pct is not None else None,
        )

    async def get_rule_frequency(self, days: int = 30) -> list[RuleFrequencyEntry]:
        from spark_advisor_gateway.api.schemas import RuleFrequencyEntry

        since = datetime.now(UTC) - timedelta(days=days)
        tasks = await self._store.list_completed_since(since)
        counter: Counter[str] = Counter()
        rule_meta: dict[str, tuple[str, Severity]] = {}
        for t in tasks:
            if not t.result:
                continue
            for r in t.result.rule_results:
                counter[r.rule_id] += 1
                rule_meta[r.rule_id] = (r.title, r.severity)
        return [
            RuleFrequencyEntry(rule_id=rid, title=rule_meta[rid][0], count=cnt, severity=rule_meta[rid][1])
            for rid, cnt in counter.most_common()
        ]

    async def get_daily_volume(self, days: int = 30) -> list[tuple[str, int]]:
        since = datetime.now(UTC) - timedelta(days=days)
        return await self._store.count_daily_since(since)

    async def get_top_issues(self, days: int = 30, limit: int = 10) -> list[TopIssueEntry]:
        from spark_advisor_gateway.api.schemas import TopIssueEntry

        since = datetime.now(UTC) - timedelta(days=days)
        tasks = await self._store.list_completed_since(since)
        counter: Counter[str] = Counter()
        rule_meta: dict[str, tuple[str, Severity]] = {}
        example_app: dict[str, str] = {}
        for t in tasks:
            if not t.result:
                continue
            for r in t.result.rule_results:
                counter[r.rule_id] += 1
                rule_meta[r.rule_id] = (r.title, r.severity)
                if r.rule_id not in example_app:
                    example_app[r.rule_id] = t.app_id
        return [
            TopIssueEntry(
                rule_id=rid,
                title=rule_meta[rid][0],
                count=cnt,
                severity=rule_meta[rid][1],
                example_app_id=example_app[rid],
            )
            for rid, cnt in counter.most_common(limit)
        ]

