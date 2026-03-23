from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, ClassVar

from sqlalchemy import DateTime, String, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class BasePollingRow(_Base):
    __abstract__ = True

    pk_column: ClassVar[str]

    processed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class BasePollingStore:
    _row_cls: type[BasePollingRow]

    def __init__(self, database_url: str, max_size: int = 10_000) -> None:
        self._engine: AsyncEngine = _create_async_engine(database_url, echo=False)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )
        self._max_size = max_size

    async def init(self) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(self._row_cls.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def _evict_if_needed(self, session: AsyncSession) -> None:
        cls = self._row_cls
        pk = self._pk_attr()
        count_result = await session.execute(select(func.count()).select_from(cls))
        total: int = count_result.scalar_one()
        if total <= self._max_size:
            return
        excess = total - self._max_size
        oldest = await session.execute(
            select(pk).order_by(cls.processed_at.asc()).limit(excess)
        )
        old_keys = [row[0] for row in oldest]
        if old_keys:
            await session.execute(delete(cls).where(pk.in_(old_keys)))

    def _pk_attr(self) -> Any:
        return getattr(self._row_cls, self._row_cls.pk_column)


def utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class ProcessedAppRow(BasePollingRow):
    __tablename__ = "processed_apps"
    pk_column: ClassVar[str] = "app_id"

    app_id: Mapped[str] = mapped_column(String(255), primary_key=True)


class ProcessedLogRow(BasePollingRow):
    __tablename__ = "processed_logs"
    pk_column: ClassVar[str] = "log_path"

    log_path: Mapped[str] = mapped_column(String(1024), primary_key=True)


class ProcessedK8sAppRow(BasePollingRow):
    __tablename__ = "processed_k8s_apps"
    pk_column: ClassVar[str] = "key"

    key: Mapped[str] = mapped_column(String(1024), primary_key=True)
