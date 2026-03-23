from __future__ import annotations

from sqlalchemy import delete, func, select

from spark_advisor_models.store import BasePollingStore, ProcessedAppRow, utcnow_naive


class PollingStore(BasePollingStore):
    _row_cls = ProcessedAppRow

    async def is_processed(self, app_id: str) -> bool:
        async with self._session_factory() as session:
            row: ProcessedAppRow | None = await session.get(ProcessedAppRow, app_id)
            return row is not None

    async def mark_processed(self, app_id: str) -> None:
        async with self._session_factory() as session, session.begin():
            existing: ProcessedAppRow | None = await session.get(ProcessedAppRow, app_id)
            if existing is None:
                session.add(ProcessedAppRow(app_id=app_id, processed_at=utcnow_naive()))
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
            now = utcnow_naive()
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
