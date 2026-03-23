from __future__ import annotations

from sqlalchemy import delete, select

from spark_advisor_models.store import BasePollingStore, ProcessedLogRow, utcnow_naive


class PollingStore(BasePollingStore):
    _row_cls = ProcessedLogRow

    async def filter_new_and_mark(self, paths: list[str]) -> list[str]:
        if not paths:
            return []
        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                select(ProcessedLogRow.log_path).where(ProcessedLogRow.log_path.in_(paths))
            )
            known = {row[0] for row in result}
            new = [p for p in paths if p not in known]
            now = utcnow_naive()
            for p in new:
                session.add(ProcessedLogRow(log_path=p, processed_at=now))
            await self._evict_if_needed(session)
            return new

    async def remove(self, path: str) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(
                delete(ProcessedLogRow).where(ProcessedLogRow.log_path == path)
            )
