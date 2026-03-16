from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_models.model import AnalysisMode, DataSource, Severity

if TYPE_CHECKING:
    from spark_advisor_models.model import ApplicationSummary


class AnalyzeRequest(BaseModel):
    app_id: str = Field(
        ...,
        min_length=1,
        max_length=128,
        pattern=r"^[a-zA-Z0-9_\-]+$",
        examples=["application_1234567890123_0001"],
    )
    mode: AnalysisMode = Field(default=AnalysisMode.AI, examples=["ai"])
    data_source: DataSource = Field(default=DataSource.HS_MANUAL, examples=["hs_manual"])
    rerun: bool = Field(default=False, examples=[False])


class ApplicationResponse(BaseModel):
    id: str = Field(examples=["application_1234567890123_0001"])
    name: str = Field(default="", examples=["DailyETL"])
    start_time: str = Field(default="", examples=["2026-03-15T10:00:00.000GMT"])
    end_time: str = Field(default="", examples=["2026-03-15T10:05:30.000GMT"])
    duration_ms: int = Field(default=0, examples=[330000])
    completed: bool = Field(default=False, examples=[True])
    spark_version: str = Field(default="", examples=["3.5.1"])
    user: str = Field(default="", examples=["hdfs"])
    analysis_count: int = Field(default=0, examples=[3])

    @classmethod
    def from_summary(cls, app: ApplicationSummary, *, analysis_count: int = 0) -> ApplicationResponse:
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
            analysis_count=analysis_count,
        )


class TaskResponse(BaseModel):
    task_id: str = Field(examples=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"])
    app_id: str = Field(examples=["application_1234567890123_0001"])
    mode: AnalysisMode = Field(examples=["ai"])
    data_source: DataSource = Field(default=DataSource.HS_MANUAL, examples=["hs_manual"])
    status: TaskStatus = Field(examples=["completed"])
    severity_counts: dict[Severity, int] | None = Field(
        default=None, examples=[{"CRITICAL": 1, "WARNING": 3, "INFO": 2}],
    )
    created_at: datetime = Field(examples=["2026-03-15T10:00:00Z"])
    started_at: datetime | None = Field(default=None, examples=["2026-03-15T10:00:01Z"])
    completed_at: datetime | None = Field(default=None, examples=["2026-03-15T10:02:15Z"])
    error: str | None = Field(default=None, examples=[None])
    result: dict[str, object] | None = Field(default=None)

    @classmethod
    def from_task(cls, task: AnalysisTask) -> TaskResponse:
        severity_counts: dict[Severity, int] | None = None
        if task.result and task.result.rule_results:
            counts: dict[Severity, int] = {}
            for r in task.result.rule_results:
                counts[r.severity] = counts.get(r.severity, 0) + 1
            severity_counts = counts

        return cls(
            task_id=task.task_id,
            app_id=task.app_id,
            mode=task.mode,
            data_source=task.data_source,
            status=task.status,
            severity_counts=severity_counts,
            created_at=task.created_at,
            started_at=task.started_at,
            completed_at=task.completed_at,
            error=task.error,
            result=task.result.model_dump(mode="json") if task.result else None,
        )


class PaginatedTaskResponse(BaseModel):
    items: list[TaskResponse]
    total: int = Field(examples=[42])
    limit: int | None = Field(examples=[50])
    offset: int = Field(examples=[0])


class PaginatedApplicationResponse(BaseModel):
    items: list[ApplicationResponse]
    total: int = Field(examples=[15])
    limit: int = Field(examples=[20])
    offset: int = Field(examples=[0])


class AnalyzeResponse(BaseModel):
    task_id: str = Field(examples=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"])
    status: TaskStatus = Field(examples=["pending"])


class TaskStatsResponse(BaseModel):
    counts: dict[TaskStatus, int] = Field(examples=[{"pending": 2, "running": 1, "completed": 10, "failed": 3}])
    total: int = Field(examples=[16])


class RuleViolationResponse(BaseModel):
    rule_id: str = Field(examples=["shuffle_partitions"])
    severity: Severity = Field(examples=["WARNING"])
    title: str = Field(examples=["Shuffle partitions too low"])
    message: str = Field(examples=["Consider increasing spark.sql.shuffle.partitions"])
    stage_id: int | None = Field(default=None, examples=[3])
    current_value: str = Field(default="", examples=["200"])
    recommended_value: str = Field(default="", examples=["800"])
    estimated_impact: str = Field(default="", examples=["~40% reduction in shuffle time"])


class ConfigComparisonEntry(BaseModel):
    parameter: str = Field(examples=["spark.sql.shuffle.partitions"])
    current_value: str = Field(examples=["200"])
    recommended_value: str = Field(examples=["800"])
    source: str = Field(examples=["rule"])


class ConfigComparisonResponse(BaseModel):
    app_id: str = Field(examples=["application_1234567890123_0001"])
    entries: list[ConfigComparisonEntry]


class StatsSummaryResponse(BaseModel):
    total: int = Field(examples=[150])
    completed: int = Field(examples=[120])
    failed: int = Field(examples=[10])
    avg_duration_seconds: float | None = Field(default=None, examples=[45.3])
    ai_usage_percent: float | None = Field(default=None, examples=[75.0])
    avg_issues_per_analysis: float | None = Field(default=None, examples=[3.2])


class RuleFrequencyEntry(BaseModel):
    rule_id: str = Field(examples=["shuffle_partitions"])
    title: str = Field(examples=["Shuffle partitions too low"])
    count: int = Field(examples=[42])
    severity: Severity = Field(examples=["WARNING"])


class RuleFrequencyResponse(BaseModel):
    items: list[RuleFrequencyEntry]
    days: int = Field(examples=[30])


class DailyVolumeEntry(BaseModel):
    date: str = Field(examples=["2026-03-15"])
    count: int = Field(examples=[12])


class DailyVolumeResponse(BaseModel):
    items: list[DailyVolumeEntry]
    days: int = Field(examples=[30])


class ModeBreakdownEntry(BaseModel):
    mode: AnalysisMode = Field(examples=["ai"])
    count: int = Field(examples=[85])


class ModeBreakdownResponse(BaseModel):
    items: list[ModeBreakdownEntry]
    days: int = Field(examples=[30])


class DataSourceBreakdownEntry(BaseModel):
    data_source: DataSource = Field(examples=["hs_manual"])
    count: int = Field(examples=[85])


class DataSourceBreakdownResponse(BaseModel):
    items: list[DataSourceBreakdownEntry]
    days: int = Field(examples=[30])


class SeverityTrendEntry(BaseModel):
    date: str = Field(examples=["2026-03-15"])
    critical: int = Field(examples=[3])
    warning: int = Field(examples=[5])
    info: int = Field(examples=[2])


class SeverityTrendResponse(BaseModel):
    items: list[SeverityTrendEntry]
    days: int = Field(examples=[30])


class TopAppEntry(BaseModel):
    app_id: str = Field(examples=["application_1234567890123_0001"])
    analysis_count: int = Field(examples=[12])


class TopAppsResponse(BaseModel):
    items: list[TopAppEntry]
    limit: int = Field(examples=[10])
    days: int = Field(examples=[30])


class DurationByModeEntry(BaseModel):
    mode: AnalysisMode = Field(examples=["ai"])
    avg_duration_seconds: float = Field(examples=[45.3])
    count: int = Field(examples=[85])


class DurationByModeResponse(BaseModel):
    items: list[DurationByModeEntry]
    days: int = Field(examples=[30])


class FailureRateTrendEntry(BaseModel):
    date: str = Field(examples=["2026-03-15"])
    total: int = Field(examples=[10])
    failed: int = Field(examples=[1])
    rate: float = Field(examples=[10.0])


class FailureRateTrendResponse(BaseModel):
    items: list[FailureRateTrendEntry]
    days: int = Field(examples=[30])


class TopIssueEntry(BaseModel):
    rule_id: str = Field(examples=["shuffle_partitions"])
    title: str = Field(examples=["Shuffle partitions too low"])
    message: str = Field(examples=["Default 200 partitions — adjust to data volume"])
    count: int = Field(examples=[42])
    severity: Severity = Field(examples=["WARNING"])
    example_app_id: str = Field(examples=["application_1234567890123_0001"])


class TopIssuesResponse(BaseModel):
    items: list[TopIssueEntry]
    limit: int = Field(examples=[10])
    days: int = Field(examples=[30])
