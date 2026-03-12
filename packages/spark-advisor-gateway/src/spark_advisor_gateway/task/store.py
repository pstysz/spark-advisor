from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine

from spark_advisor_gateway.task.db_models import Base, TaskRow
from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_models.model import AnalysisResult


class SqlAlchemyTaskStore:
    def __init__(self, database_url: str) -> None:
        self._engine: AsyncEngine = _create_async_engine(database_url, echo=False)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    async def init(self) -> None:
        async with self._engine.begin() as conn: # type: ignore[attr-defined]
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def create(self, task: AnalysisTask) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(_to_row(task))

    async def get(self, task_id: str) -> AnalysisTask | None:
        async with self._session_factory() as session:
            row: TaskRow | None = await session.get(TaskRow, task_id)
            return _to_task(row) if row else None

    async def update(self, task: AnalysisTask) -> None:
        async with self._session_factory() as session, session.begin():
            row: TaskRow | None = await session.get(TaskRow, task.task_id)
            if row is None:
                return
            _apply_task_to_row(task, row)

    async def list_recent(self, limit: int) -> list[AnalysisTask]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow).order_by(TaskRow.created_at.desc(), TaskRow.task_id.desc()).limit(limit)
            )
            return [_to_task(row) for row in result.scalars()]

    async def list_filtered(
            self,
            *,
            limit: int | None = 50,
            offset: int = 0,
            status: TaskStatus | None = None,
            app_id: str | None = None,
    ) -> tuple[list[AnalysisTask], int]:
        async with self._session_factory() as session, session.begin():
            base = select(TaskRow)
            if status is not None:
                base = base.where(TaskRow.status == status.value)
            if app_id is not None:
                base = base.where(TaskRow.app_id == app_id)

            count_result = await session.execute(select(func.count()).select_from(base.subquery()))
            total: int = count_result.scalar_one()

            query = base.order_by(TaskRow.created_at.desc(), TaskRow.task_id.desc()).offset(offset)
            if limit is not None:
                query = query.limit(limit)

            result = await session.execute(query)
            tasks = [_to_task(row) for row in result.scalars()]
            return tasks, total

    async def count_by_status(self) -> dict[TaskStatus, int]:
        async with self._session_factory() as session:
            result = await session.execute(select(TaskRow.status, func.count().label("cnt")).group_by(TaskRow.status))
            return {TaskStatus(row.status): row.cnt for row in result}


def _to_row(task: AnalysisTask) -> TaskRow:
    return TaskRow(
        task_id=task.task_id,
        app_id=task.app_id,
        status=task.status.value,
        created_at=_strip_tz(task.created_at),
        started_at=_strip_tz(task.started_at),
        completed_at=_strip_tz(task.completed_at),
        error=task.error,
        result_json=task.result.model_dump_json() if task.result else None,
    )


def _apply_task_to_row(task: AnalysisTask, row: TaskRow) -> None:
    row.status = task.status.value
    row.started_at = _strip_tz(task.started_at)
    row.completed_at = _strip_tz(task.completed_at)
    row.error = task.error
    row.result_json = task.result.model_dump_json() if task.result else None


def _to_task(row: TaskRow) -> AnalysisTask:
    result: AnalysisResult | None = None
    if row.result_json is not None:
        result = AnalysisResult.model_validate_json(row.result_json)

    return AnalysisTask(
        task_id=row.task_id,
        app_id=row.app_id,
        status=TaskStatus(row.status),
        created_at=row.created_at.replace(tzinfo=UTC),
        started_at=_attach_tz(row.started_at),
        completed_at=_attach_tz(row.completed_at),
        error=row.error,
        result=result,
    )


def _strip_tz(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).replace(tzinfo=None)


def _attach_tz(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=UTC)
