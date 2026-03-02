from datetime import datetime

from pydantic import BaseModel

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus


class AnalyzeRequest(BaseModel):
    app_id: str


class ApplicationResponse(BaseModel):
    id: str
    name: str = ""
    start_time: str = ""
    end_time: str = ""
    duration_ms: int = 0
    completed: bool = False
    spark_version: str = ""
    user: str = ""

    @classmethod
    def from_hs_data(cls, data: dict[str, object]) -> "ApplicationResponse":
        attempts = data.get("attempts") or []
        first = attempts[0] if isinstance(attempts, list) and attempts else {}
        if not isinstance(first, dict):
            first = {}
        return cls(
            id=str(data.get("id", "")),
            name=str(data.get("name", "")),
            start_time=str(first.get("startTime", "")),
            end_time=str(first.get("endTime", "")),
            duration_ms=int(first.get("duration", 0)),
            completed=bool(first.get("completed", False)),
            spark_version=str(first.get("appSparkVersion", "")),
            user=str(first.get("sparkUser", "")),
        )


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
