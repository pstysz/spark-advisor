from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class ProcessedLogRow(_Base):
    __tablename__ = "processed_logs"

    log_path: Mapped[str] = mapped_column(String(1024), primary_key=True)
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

    async def filter_new_and_mark(self, paths: list[str]) -> list[str]:
        if not paths:
            return []
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                select(ProcessedLogRow.log_path).where(ProcessedLogRow.log_path.in_(paths))
            )
            known = {row[0] for row in result}
            new = [p for p in paths if p not in known]
            now = _now()
            for p in new:
                session.add(ProcessedLogRow(log_path=p, processed_at=now))
            await self._evict_if_needed(session)
            return new

    async def remove(self, path: str) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                delete(ProcessedLogRow).where(ProcessedLogRow.log_path == path)
            )

    async def _evict_if_needed(self, session: AsyncSession) -> None:
        count_result = await session.execute(select(func.count()).select_from(ProcessedLogRow))
        total: int = count_result.scalar_one()
        if total <= self._max_size:
            return
        excess = total - self._max_size
        oldest = await session.execute(
            select(ProcessedLogRow.log_path).order_by(ProcessedLogRow.processed_at.asc()).limit(excess)
        )
        old_paths = [row[0] for row in oldest]
        if old_paths:
            await session.execute(delete(ProcessedLogRow).where(ProcessedLogRow.log_path.in_(old_paths)))


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
