from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import orjson
from fastapi import APIRouter, HTTPException, Request

from spark_advisor_gateway.api.schemas import AnalyzeRequest, ApplicationResponse, TaskResponse

if TYPE_CHECKING:
    import nats.aio.client

    from spark_advisor_gateway.config import GatewaySettings
    from spark_advisor_gateway.task.executor import TaskExecutor
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)


def create_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/applications")
    async def list_applications(request: Request, limit: int = 20) -> list[ApplicationResponse]:
        nc: nats.aio.client.Client = request.app.state.nc
        settings: GatewaySettings = request.app.state.settings
        reply = await nc.request(
            settings.nats.list_apps_subject,
            orjson.dumps({"limit": limit}),
            timeout=settings.nats.list_apps_timeout,
        )
        data = orjson.loads(reply.data)
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=502, detail=data["error"])
        return [ApplicationResponse.from_hs_data(app) for app in data]

    @router.post("/analyze", status_code=202)
    async def analyze(body: AnalyzeRequest, request: Request) -> dict[str, str]:
        manager: TaskManager = request.app.state.task_manager
        executor: TaskExecutor = request.app.state.task_executor
        task = manager.create(body.app_id)
        executor.submit(task.task_id, body.app_id, body.mode)
        return {"task_id": task.task_id, "status": task.status.value}

    @router.get("/tasks/{task_id}")
    async def get_task(task_id: str, request: Request) -> TaskResponse:
        manager: TaskManager = request.app.state.task_manager
        task = manager.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskResponse.from_task(task)

    @router.get("/tasks")
    async def list_tasks(request: Request, limit: int = 50) -> list[TaskResponse]:
        manager: TaskManager = request.app.state.task_manager
        return [TaskResponse.from_task(t) for t in manager.list_recent(limit)]

    return router
