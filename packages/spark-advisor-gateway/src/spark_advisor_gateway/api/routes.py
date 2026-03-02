from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request

from spark_advisor_gateway.api.schemas import AnalyzeRequest, TaskResponse

if TYPE_CHECKING:
    from spark_advisor_gateway.task.executor import TaskExecutor
    from spark_advisor_gateway.task.manager import TaskManager


def create_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.post("/analyze", status_code=202)
    async def analyze(body: AnalyzeRequest, request: Request) -> dict[str, str]:
        manager: TaskManager = request.app.state.task_manager
        executor: TaskExecutor = request.app.state.task_executor
        task = manager.create(body.app_id)
        executor.submit(task.task_id, body.app_id)
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
