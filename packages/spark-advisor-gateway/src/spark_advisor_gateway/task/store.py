from __future__ import annotations

import contextlib
from datetime import UTC, datetime

from sqlalchemy import DateTime, String, Text, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_models.model import AnalysisMode, AnalysisResult, DataSource


class _Base(DeclarativeBase):
    pass


class TaskRow(_Base):
    __tablename__ = "tasks"

    task_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(10), nullable=False, default="ai")
    data_source: Mapped[str] = mapped_column(String(20), nullable=False, default="hs_manual")
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class TaskStore:
    def __init__(self, database_url: str) -> None:
        self._engine: AsyncEngine = _create_async_engine(database_url, echo=False)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )

    async def init(self) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(_Base.metadata.create_all)
            with contextlib.suppress(Exception):
                await conn.execute(
                    text("ALTER TABLE tasks ADD COLUMN data_source VARCHAR(20) NOT NULL DEFAULT 'hs_manual'")
                )

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
            data_source: DataSource | None = None,
    ) -> tuple[list[AnalysisTask], int]:
        async with self._session_factory() as session, session.begin():
            base = select(TaskRow)
            if status is not None:
                base = base.where(TaskRow.status == status.value)
            if app_id is not None:
                base = base.where(TaskRow.app_id == app_id)
            if data_source is not None:
                base = base.where(TaskRow.data_source == data_source.value)

            count_result = await session.execute(select(func.count()).select_from(base.subquery()))
            total: int = count_result.scalar_one()

            query = base.order_by(TaskRow.created_at.desc(), TaskRow.task_id.desc()).offset(offset)
            if limit is not None:
                query = query.limit(limit)

            result = await session.execute(query)
            tasks = [_to_task(row) for row in result.scalars()]
            return tasks, total

    async def find_latest_by_app_id(self, app_id: str) -> AnalysisTask | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow)
                .where(TaskRow.app_id == app_id)
                .order_by(TaskRow.created_at.desc(), TaskRow.task_id.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            return _to_task(row) if row else None

    async def count_by_app_ids(self, app_ids: list[str]) -> dict[str, int]:
        if not app_ids:
            return {}
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow.app_id, func.count().label("cnt"))
                .where(TaskRow.app_id.in_(app_ids))
                .group_by(TaskRow.app_id)
            )
            return {row.app_id: row.cnt for row in result}

    async def count_by_status(self) -> dict[TaskStatus, int]:
        async with self._session_factory() as session:
            result = await session.execute(select(TaskRow.status, func.count().label("cnt")).group_by(TaskRow.status))
            return {TaskStatus(row.status): row.cnt for row in result}

    async def list_completed_since(self, since: datetime) -> list[AnalysisTask]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow)
                .where(TaskRow.status == TaskStatus.COMPLETED.value)
                .where(TaskRow.completed_at >= _strip_tz(since))
                .order_by(TaskRow.completed_at.desc())
            )
            return [_to_task(row) for row in result.scalars()]

    async def count_daily_since(self, since: datetime) -> list[tuple[str, int]]:
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT date(created_at) AS day, count(*) AS cnt "
                    "FROM tasks WHERE created_at >= :since "
                    "GROUP BY day ORDER BY day"
                ),
                {"since": _strip_tz(since)},
            )
            return [(row.day, row.cnt) for row in result]

    async def count_by_mode_since(self, since: datetime) -> list[tuple[str, int]]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow.mode, func.count().label("cnt"))
                .where(TaskRow.created_at >= _strip_tz(since))
                .group_by(TaskRow.mode)
            )
            return [(row.mode, row.cnt) for row in result]

    async def count_by_data_source_since(self, since: datetime) -> list[tuple[str, int]]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow.data_source, func.count().label("cnt"))
                .where(TaskRow.created_at >= _strip_tz(since))
                .group_by(TaskRow.data_source)
            )
            return [(row.data_source, row.cnt) for row in result]

    async def list_all_since(self, since: datetime) -> list[AnalysisTask]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow)
                .where(TaskRow.created_at >= _strip_tz(since))
                .order_by(TaskRow.created_at.desc())
            )
            return [_to_task(row) for row in result.scalars()]

    async def top_app_ids_since(self, since: datetime, limit: int) -> list[tuple[str, int]]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow.app_id, func.count().label("cnt"))
                .where(TaskRow.created_at >= _strip_tz(since))
                .group_by(TaskRow.app_id)
                .order_by(func.count().desc())
                .limit(limit)
            )
            return [(row.app_id, row.cnt) for row in result]

    async def count_daily_by_status_since(self, since: datetime) -> list[tuple[str, int, int]]:
        async with self._session_factory() as session:
            result = await session.execute(
                text(
                    "SELECT date(created_at) AS day, count(*) AS total, "
                    "sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed "
                    "FROM tasks WHERE created_at >= :since "
                    "GROUP BY day ORDER BY day"
                ),
                {"since": _strip_tz(since)},
            )
            return [(row.day, row.total, row.failed) for row in result]

    async def count_since(self, since: datetime) -> dict[TaskStatus, int]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(TaskRow.status, func.count().label("cnt"))
                .where(TaskRow.created_at >= _strip_tz(since))
                .group_by(TaskRow.status)
            )
            return {TaskStatus(row.status): row.cnt for row in result}


def _to_row(task: AnalysisTask) -> TaskRow:
    return TaskRow(
        task_id=task.task_id,
        app_id=task.app_id,
        mode=task.mode.value,
        data_source=task.data_source.value,
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
        mode=AnalysisMode(row.mode),
        data_source=DataSource(row.data_source),
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
