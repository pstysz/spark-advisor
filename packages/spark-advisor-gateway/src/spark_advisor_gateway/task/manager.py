from __future__ import annotations

import logging
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_models.model import AnalysisMode, DataSource

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from spark_advisor_gateway.api.schemas import (
        DataSourceBreakdownEntry,
        DurationByModeEntry,
        FailureRateTrendEntry,
        ModeBreakdownEntry,
        RuleFrequencyEntry,
        SeverityTrendEntry,
        StatsSummaryResponse,
        TopAppEntry,
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

    async def create(
        self,
        app_id: str,
        mode: AnalysisMode = AnalysisMode.AI,
        data_source: DataSource = DataSource.HS_MANUAL,
    ) -> AnalysisTask:
        now = datetime.now(UTC)
        task_id = f"{time.time_ns()}_{app_id}"
        task = AnalysisTask(
            task_id=task_id, app_id=app_id, mode=mode,
            data_source=data_source, created_at=now,
        )
        await self._store.create(task)
        return task

    async def create_if_not_active(
        self,
        app_id: str,
        *,
        rerun: bool = False,
        mode: AnalysisMode = AnalysisMode.AI,
        data_source: DataSource = DataSource.HS_MANUAL,
    ) -> tuple[AnalysisTask, bool] | None:
        """Try to create a task for app_id with deduplication.

        Returns:
            (task, True)  — new task created, proceed with analysis
            (task, False) — existing active task found, skip (dedup)
            None          — rerun requested but task still active, cannot proceed
        """
        existing = await self._store.find_latest_by_app_id(app_id)
        if existing is None:
            return await self.create(app_id, mode, data_source), True

        if not rerun:
            if existing.status not in _TERMINAL_STATUSES:
                logger.info("Skipping %s — active task %s (%s)", app_id, existing.task_id, existing.status)
                return existing, False
            return await self.create(app_id, mode, data_source), True

        if existing.status not in _TERMINAL_STATUSES:
            logger.info("Cannot rerun %s — task %s still %s", app_id, existing.task_id, existing.status)
            return None

        return await self.create(app_id, mode, data_source), True

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
        data_source: DataSource | None = None,
    ) -> tuple[list[AnalysisTask], int]:
        return await self._store.list_filtered(
            limit=limit, offset=offset, status=status, app_id=app_id, data_source=data_source,
        )

    async def count_by_app_ids(self, app_ids: list[str]) -> dict[str, int]:
        return await self._store.count_by_app_ids(app_ids)

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
            "data_source": task.data_source.value,
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
        avg_issues: float | None = None
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
            issue_counts = [len(t.result.rule_results) for t in completed_tasks if t.result]
            avg_issues = sum(issue_counts) / len(issue_counts) if issue_counts else None

        return StatsSummaryResponse(
            total=total,
            completed=completed,
            failed=failed,
            avg_duration_seconds=round(avg_duration, 2) if avg_duration is not None else None,
            ai_usage_percent=round(ai_usage_pct, 1) if ai_usage_pct is not None else None,
            avg_issues_per_analysis=round(avg_issues, 1) if avg_issues is not None else None,
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
        rule_meta: dict[str, tuple[str, str, Severity]] = {}
        example_app: dict[str, str] = {}
        for t in tasks:
            if not t.result:
                continue
            for r in t.result.rule_results:
                counter[r.rule_id] += 1
                rule_meta[r.rule_id] = (r.title, r.message, r.severity)
                if r.rule_id not in example_app:
                    example_app[r.rule_id] = t.app_id
        return [
            TopIssueEntry(
                rule_id=rid,
                title=rule_meta[rid][0],
                message=rule_meta[rid][1],
                count=cnt,
                severity=rule_meta[rid][2],
                example_app_id=example_app[rid],
            )
            for rid, cnt in counter.most_common(limit)
        ]

    async def get_mode_breakdown(self, days: int = 30) -> list[ModeBreakdownEntry]:
        from spark_advisor_gateway.api.schemas import ModeBreakdownEntry

        since = datetime.now(UTC) - timedelta(days=days)
        rows = await self._store.count_by_mode_since(since)
        return [ModeBreakdownEntry(mode=AnalysisMode(mode), count=cnt) for mode, cnt in rows]

    async def get_data_source_breakdown(self, days: int = 30) -> list[DataSourceBreakdownEntry]:
        from spark_advisor_gateway.api.schemas import DataSourceBreakdownEntry

        since = datetime.now(UTC) - timedelta(days=days)
        rows = await self._store.count_by_data_source_since(since)
        return [DataSourceBreakdownEntry(data_source=DataSource(ds), count=cnt) for ds, cnt in rows]

    async def get_severity_trend(self, days: int = 30) -> list[SeverityTrendEntry]:
        from spark_advisor_gateway.api.schemas import SeverityTrendEntry
        from spark_advisor_models.model import Severity as Sev

        since = datetime.now(UTC) - timedelta(days=days)
        tasks = await self._store.list_completed_since(since)
        daily: dict[str, dict[str, int]] = {}
        for t in tasks:
            if not t.result or not t.completed_at:
                continue
            day = t.completed_at.strftime("%Y-%m-%d")
            if day not in daily:
                daily[day] = {"critical": 0, "warning": 0, "info": 0}
            for r in t.result.rule_results:
                if r.severity == Sev.CRITICAL:
                    daily[day]["critical"] += 1
                elif r.severity == Sev.WARNING:
                    daily[day]["warning"] += 1
                else:
                    daily[day]["info"] += 1
        return [
            SeverityTrendEntry(date=day, **counts)
            for day, counts in sorted(daily.items())
        ]

    async def get_top_apps(self, days: int = 30, limit: int = 10) -> list[TopAppEntry]:
        from spark_advisor_gateway.api.schemas import TopAppEntry

        since = datetime.now(UTC) - timedelta(days=days)
        rows = await self._store.top_app_ids_since(since, limit)
        return [TopAppEntry(app_id=app_id, analysis_count=cnt) for app_id, cnt in rows]

    async def get_duration_by_mode(self, days: int = 30) -> list[DurationByModeEntry]:
        from spark_advisor_gateway.api.schemas import DurationByModeEntry

        since = datetime.now(UTC) - timedelta(days=days)
        tasks = await self._store.list_completed_since(since)
        mode_durations: dict[AnalysisMode, list[float]] = {}
        for t in tasks:
            if t.started_at and t.completed_at:
                mode_durations.setdefault(t.mode, []).append(
                    (t.completed_at - t.started_at).total_seconds()
                )
        return [
            DurationByModeEntry(
                mode=mode,
                avg_duration_seconds=round(sum(durs) / len(durs), 2),
                count=len(durs),
            )
            for mode, durs in sorted(mode_durations.items(), key=lambda x: x[0].value)
        ]

    async def get_failure_rate_trend(self, days: int = 30) -> list[FailureRateTrendEntry]:
        from spark_advisor_gateway.api.schemas import FailureRateTrendEntry

        since = datetime.now(UTC) - timedelta(days=days)
        rows = await self._store.count_daily_by_status_since(since)
        return [
            FailureRateTrendEntry(
                date=day,
                total=total,
                failed=failed,
                rate=round((failed / total) * 100, 1) if total > 0 else 0.0,
            )
            for day, total, failed in rows
        ]

