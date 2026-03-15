from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_models.model import AnalysisMode

if TYPE_CHECKING:
    from spark_advisor_models.model import ApplicationSummary


class AnalyzeRequest(BaseModel):
    app_id: str = Field(..., min_length=1)
    mode: AnalysisMode = AnalysisMode.AI
    rerun: bool = False


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
    def from_summary(cls, app: ApplicationSummary) -> ApplicationResponse:
        latest = app.latest_attempt
        return cls(
            id=app.id,
            name=app.name,
            start_time=latest.start_time if latest else "",
            end_time=latest.end_time if latest else "",
            duration_ms=latest.duration if latest else 0,
            completed=latest.completed if latest else False,
            spark_version=latest.app_spark_version if latest else "",
            user=latest.spark_user if latest else "",
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
    def from_task(cls, task: AnalysisTask) -> TaskResponse:
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


class PaginatedTaskResponse(BaseModel):
    items: list[TaskResponse]
    total: int
    limit: int | None
    offset: int


class AnalyzeResponse(BaseModel):
    task_id: str
    status: TaskStatus


class TaskStatsResponse(BaseModel):
    counts: dict[TaskStatus, int]
    total: int
