from datetime import datetime

from pydantic import BaseModel

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus


class AnalyzeRequest(BaseModel):
    app_id: str


class TaskResponse(BaseModel):
    task_id: str
    app_id: str
    status: TaskStatus
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    result: dict[str, object] | None = None

    @classmethod
    def from_task(cls, task: AnalysisTask) -> "TaskResponse":
        return cls(
            task_id=task.task_id,
            app_id=task.app_id,
            status=task.status,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error=task.error,
            result=task.result.model_dump(mode="json") if task.result else None,
        )
