from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Annotated, Any

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from spark_advisor_gateway.api.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ApplicationResponse,
    PaginatedApplicationResponse,
    PaginatedTaskResponse,
    TaskResponse,
    TaskStatsResponse,
)
from spark_advisor_gateway.config import GatewaySettings, StateKey
from spark_advisor_gateway.task.models import TaskStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from spark_advisor_gateway.task.executor import TaskExecutor
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({TaskStatus.COMPLETED, TaskStatus.FAILED})


def _sse_event(event: str, data: object) -> str:
    return f"event: {event}\ndata: {orjson.dumps(data).decode()}\n\n"


async def _task_event_stream(
        manager: TaskManager,
        task_id: str,
        *,
        timeout: float,
        poll_interval: float,
) -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    last_status: TaskStatus | None = None

    while loop.time() < deadline:
        task = await manager.get(task_id)
        if not task:
            yield _sse_event("error", {"detail": "Task not found"})
            return

        if task.status != last_status:
            last_status = task.status
            yield _sse_event("status", TaskResponse.from_task(task).model_dump(mode="json"))

        if last_status in _TERMINAL_STATUSES:
            return

        await asyncio.sleep(poll_interval)

    yield _sse_event("timeout", {"detail": "Stream timeout"})


def _from_state(key: StateKey) -> Callable[..., Any]:
    def _dep(request: Request) -> Any:
        return getattr(request.app.state, key)

    return _dep


ManagerDep = Annotated["TaskManager", Depends(_from_state(StateKey.TASK_MANAGER))]
ExecutorDep = Annotated["TaskExecutor", Depends(_from_state(StateKey.TASK_EXECUTOR))]
SettingsDep = Annotated[GatewaySettings, Depends(_from_state(StateKey.SETTINGS))]


def create_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/applications")
    async def list_applications(
            executor: ExecutorDep,
            limit: int = Query(default=20, ge=1, le=500),
            offset: int = Query(default=0, ge=0),
    ) -> PaginatedApplicationResponse:
        all_apps = await executor.list_applications(offset + limit)
        page = all_apps[offset:offset + limit]
        return PaginatedApplicationResponse(
            items=[ApplicationResponse.from_summary(app) for app in page],
            total=len(all_apps),
            limit=limit,
            offset=offset,
        )

    @router.post("/analyze")
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

    @router.get("/tasks/stats")
    async def task_stats(manager: ManagerDep) -> TaskStatsResponse:
        counts = await manager.count_by_status()
        total = sum(counts.values())
        return TaskStatsResponse(counts=counts, total=total)

    @router.get("/tasks/{task_id}/stream")
    async def stream_task(task_id: str, manager: ManagerDep, settings: SettingsDep) -> StreamingResponse:
        stream = _task_event_stream(
            manager, task_id, timeout=settings.task_stream_timeout, poll_interval=settings.task_poll_interval
        )
        return StreamingResponse(stream, media_type="text/event-stream")

    @router.get("/tasks/{task_id}")
    async def get_task(task_id: str, manager: ManagerDep) -> TaskResponse:
        task = await manager.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskResponse.from_task(task)

    @router.get("/tasks")
    async def list_tasks(
            manager: ManagerDep,
            limit: int = Query(default=50, ge=1, le=500),
            offset: int = Query(default=0, ge=0),
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

    return router
