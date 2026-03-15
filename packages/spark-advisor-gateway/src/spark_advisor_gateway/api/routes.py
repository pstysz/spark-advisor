from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from spark_advisor_gateway.api.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ApplicationResponse,
    ConfigComparisonEntry,
    ConfigComparisonResponse,
    DailyVolumeEntry,
    DailyVolumeResponse,
    PaginatedApplicationResponse,
    PaginatedTaskResponse,
    RuleFrequencyResponse,
    RuleViolationResponse,
    StatsSummaryResponse,
    TaskResponse,
    TaskStatsResponse,
    TopIssuesResponse,
)
from spark_advisor_gateway.config import StateKey
from spark_advisor_gateway.task.models import TaskStatus

if TYPE_CHECKING:
    from collections.abc import Callable

    from spark_advisor_gateway.task.executor import TaskExecutor
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)


def _from_state(key: StateKey) -> Callable[..., Any]:
    def _dep(request: Request) -> Any:
        return getattr(request.app.state, key)

    return _dep


ManagerDep = Annotated["TaskManager", Depends(_from_state(StateKey.TASK_MANAGER))]
ExecutorDep = Annotated["TaskExecutor", Depends(_from_state(StateKey.TASK_EXECUTOR))]


def create_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get(
        "/applications",
        summary="List Spark applications from History Server",
        tags=["applications"],
    )
    async def list_applications(
            executor: ExecutorDep,
            limit: int = Query(default=20, ge=1, le=500, description="Maximum number of applications to return"),
            offset: int = Query(default=0, ge=0, description="Number of applications to skip"),
    ) -> PaginatedApplicationResponse:
        all_apps = await executor.list_applications(offset + limit)
        page = all_apps[offset:offset + limit]
        return PaginatedApplicationResponse(
            items=[ApplicationResponse.from_summary(app) for app in page],
            total=len(all_apps),
            limit=limit,
            offset=offset,
        )

    @router.post(
        "/analyze",
        summary="Submit analysis request",
        description="Creates an async analysis task. Returns 202 for new tasks, 409 if a task is already active.",
        tags=["analysis"],
    )
    async def analyze(
            body: AnalyzeRequest,
            manager: ManagerDep,
            executor: ExecutorDep,
            response: Response,
    ) -> AnalyzeResponse:
        result = await manager.create_if_not_active(body.app_id, rerun=body.rerun)
        if result is None:
            raise HTTPException(status_code=409, detail="Task is still running, cannot rerun")
        task, created = result
        if not created:
            response.status_code = 409
            return AnalyzeResponse(task_id=task.task_id, status=task.status)
        executor.submit(task.task_id, body.app_id, body.mode)
        response.status_code = 202
        return AnalyzeResponse(task_id=task.task_id, status=task.status)

    @router.get(
        "/tasks/stats",
        summary="Get task count by status",
        tags=["tasks"],
    )
    async def task_stats(manager: ManagerDep) -> TaskStatsResponse:
        counts = await manager.count_by_status()
        total = sum(counts.values())
        return TaskStatsResponse(counts=counts, total=total)

    @router.get(
        "/tasks/{task_id}",
        summary="Get task details",
        tags=["tasks"],
    )
    async def get_task(task_id: str, manager: ManagerDep) -> TaskResponse:
        task = await manager.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskResponse.from_task(task)

    @router.get(
        "/tasks",
        summary="List tasks with filtering and pagination",
        tags=["tasks"],
    )
    async def list_tasks(
            manager: ManagerDep,
            limit: int = Query(default=50, ge=1, le=500, description="Maximum number of tasks to return"),
            offset: int = Query(default=0, ge=0, description="Number of tasks to skip"),
            status: TaskStatus | None = None,
            app_id: str | None = None,
    ) -> PaginatedTaskResponse:
        tasks, total = await manager.list_filtered(limit=limit, offset=offset, status=status, app_id=app_id)
        return PaginatedTaskResponse(
            items=[TaskResponse.from_task(t) for t in tasks],
            total=total,
            limit=limit,
            offset=offset,
        )

    @router.get(
        "/apps/{app_id}/history",
        summary="Get analysis history for an application",
        tags=["applications"],
    )
    async def app_history(
            app_id: str,
            manager: ManagerDep,
            limit: int = Query(default=50, ge=1, le=500, description="Maximum number of tasks to return"),
            offset: int = Query(default=0, ge=0, description="Number of tasks to skip"),
    ) -> PaginatedTaskResponse:
        tasks, total = await manager.list_filtered(limit=limit, offset=offset, app_id=app_id)
        return PaginatedTaskResponse(
            items=[TaskResponse.from_task(t) for t in tasks],
            total=total,
            limit=limit,
            offset=offset,
        )

    @router.get(
        "/tasks/{task_id}/rules",
        summary="Get rule violations for a completed task",
        description="Returns rule violations from the analysis result. Returns 409 if the task is not completed.",
        tags=["tasks"],
    )
    async def task_rules(task_id: str, manager: ManagerDep) -> list[RuleViolationResponse]:
        task = await manager.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status != TaskStatus.COMPLETED or task.result is None:
            raise HTTPException(status_code=409, detail="Task is not completed")
        return [
            RuleViolationResponse(
                rule_id=r.rule_id,
                severity=r.severity,
                title=r.title,
                message=r.message,
                stage_id=r.stage_id,
                current_value=r.current_value,
                recommended_value=r.recommended_value,
                estimated_impact=r.estimated_impact,
            )
            for r in task.result.rule_results
        ]

    @router.get(
        "/tasks/{task_id}/config",
        summary="Get configuration comparison for a completed task",
        description="Merges rule-based and AI-based config recommendations. Returns 409 if the task is not completed.",
        tags=["tasks"],
    )
    async def task_config(task_id: str, manager: ManagerDep) -> ConfigComparisonResponse:
        task = await manager.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if task.status != TaskStatus.COMPLETED or task.result is None:
            raise HTTPException(status_code=409, detail="Task is not completed")
        entries: dict[str, ConfigComparisonEntry] = {}
        for r in task.result.rule_results:
            if r.recommended_value:
                param = r.rule_id
                if r.current_value:
                    param = r.current_value.split("=")[0].strip() if "=" in r.current_value else r.rule_id
                entries[param] = ConfigComparisonEntry(
                    parameter=param,
                    current_value=r.current_value,
                    recommended_value=r.recommended_value,
                    source="rule",
                )
        if task.result.ai_report:
            for param, value in task.result.ai_report.suggested_config.items():
                entries[param] = ConfigComparisonEntry(
                    parameter=param,
                    current_value=entries[param].current_value if param in entries else "",
                    recommended_value=value,
                    source="ai",
                )
            for rec in task.result.ai_report.recommendations:
                if rec.parameter:
                    entries[rec.parameter] = ConfigComparisonEntry(
                        parameter=rec.parameter,
                        current_value=rec.current_value,
                        recommended_value=rec.recommended_value,
                        source="ai",
                    )
        return ConfigComparisonResponse(app_id=task.app_id, entries=list(entries.values()))

    @router.get(
        "/stats/summary",
        summary="Get analysis statistics summary",
        description="Aggregates task counts, average duration, and AI usage over a time window.",
        tags=["statistics"],
    )
    async def stats_summary(
            manager: ManagerDep,
            days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    ) -> StatsSummaryResponse:
        return await manager.get_stats_summary(days)

    @router.get(
        "/stats/rules",
        summary="Get rule violation frequency",
        description="Counts how often each rule was triggered across completed analyses.",
        tags=["statistics"],
    )
    async def stats_rules(
            manager: ManagerDep,
            days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    ) -> RuleFrequencyResponse:
        items = await manager.get_rule_frequency(days)
        return RuleFrequencyResponse(items=items, days=days)

    @router.get(
        "/stats/daily-volume",
        summary="Get daily analysis volume",
        tags=["statistics"],
    )
    async def stats_daily_volume(
            manager: ManagerDep,
            days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
    ) -> DailyVolumeResponse:
        rows = await manager.get_daily_volume(days)
        return DailyVolumeResponse(
            items=[DailyVolumeEntry(date=date, count=count) for date, count in rows],
            days=days,
        )

    @router.get(
        "/stats/top-issues",
        summary="Get most common issues",
        description="Returns top N most frequently triggered rules with example app IDs.",
        tags=["statistics"],
    )
    async def stats_top_issues(
            manager: ManagerDep,
            days: int = Query(default=30, ge=1, le=365, description="Number of days to look back"),
            limit: int = Query(default=10, ge=1, le=100, description="Maximum number of issues to return"),
    ) -> TopIssuesResponse:
        items = await manager.get_top_issues(days, limit)
        return TopIssuesResponse(items=items, days=days, limit=limit)

    return router
