from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class ProcessedAppRow(_Base):
    __tablename__ = "processed_apps"

    app_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class PollingStore:

    def __init__(self, database_url: str, max_size: int = 10_000) -> None:
        self._engine: AsyncEngine = _create_async_engine(database_url, echo=False)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )
        self._max_size = max_size

    async def init(self) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(_Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def is_processed(self, app_id: str) -> bool:
        async with self._session_factory() as session:
            row: ProcessedAppRow | None = await session.get(ProcessedAppRow, app_id)
            return row is not None

    async def mark_processed(self, app_id: str) -> None:
        async with self._session_factory() as session, session.begin():
            existing: ProcessedAppRow | None = await session.get(ProcessedAppRow, app_id)
            if existing is None:
                session.add(ProcessedAppRow(app_id=app_id, processed_at=_now()))
            await self._evict_if_needed(session)

    async def filter_new(self, app_ids: list[str]) -> list[str]:
        if not app_ids:
            return []
        async with self._session_factory() as session:
            result = await session.execute(
                select(ProcessedAppRow.app_id).where(ProcessedAppRow.app_id.in_(app_ids))
            )
            known = {row[0] for row in result}
            return [aid for aid in app_ids if aid not in known]

    async def filter_new_and_mark(self, app_ids: list[str]) -> list[str]:
        if not app_ids:
            return []
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                select(ProcessedAppRow.app_id).where(ProcessedAppRow.app_id.in_(app_ids))
            )
            known = {row[0] for row in result}
            new = [aid for aid in app_ids if aid not in known]
            now = _now()
            for aid in new:
                session.add(ProcessedAppRow(app_id=aid, processed_at=now))
            await self._evict_if_needed(session)
            return new

    async def remove(self, app_id: str) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                delete(ProcessedAppRow).where(ProcessedAppRow.app_id == app_id)
            )

    async def processed_count(self) -> int:
        async with self._session_factory() as session:
            result = await session.execute(select(func.count()).select_from(ProcessedAppRow))
            return result.scalar_one()

    async def _evict_if_needed(self, session: AsyncSession) -> None:
        count_result = await session.execute(select(func.count()).select_from(ProcessedAppRow))
        total: int = count_result.scalar_one()
        if total <= self._max_size:
            return
        excess = total - self._max_size
        oldest = await session.execute(
            select(ProcessedAppRow.app_id).order_by(ProcessedAppRow.processed_at.asc()).limit(excess)
        )
        old_ids = [row[0] for row in oldest]
        if old_ids:
            await session.execute(delete(ProcessedAppRow).where(ProcessedAppRow.app_id.in_(old_ids)))


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
