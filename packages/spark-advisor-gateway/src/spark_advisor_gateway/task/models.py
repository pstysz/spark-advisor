from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from spark_advisor_models.model import AnalysisMode, AnalysisResult, DataSource

__all__ = ["AnalysisMode", "AnalysisResult", "AnalysisTask", "DataSource", "TaskStatus"]


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AnalysisTask:
    task_id: str
    app_id: str
    mode: AnalysisMode = AnalysisMode.AI
    data_source: DataSource = DataSource.HS_MANUAL
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    result: AnalysisResult | None = None
