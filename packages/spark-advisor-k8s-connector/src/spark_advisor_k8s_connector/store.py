from __future__ import annotations

from sqlalchemy import delete, select

from spark_advisor_models.store import BasePollingStore, ProcessedK8sAppRow, utcnow_naive


class K8sPollingStore(BasePollingStore):
    _row_cls = ProcessedK8sAppRow

    @staticmethod
    def _make_key(namespace: str, name: str, completed_at: str) -> str:
        return f"{namespace}/{name}@{completed_at}"

    async def is_processed(self, namespace: str, name: str, completed_at: str) -> bool:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session:
            result = await session.execute(
                select(ProcessedK8sAppRow.key).where(ProcessedK8sAppRow.key == key)
            )
            return result.scalar_one_or_none() is not None

    async def mark_processed(self, namespace: str, name: str, completed_at: str) -> None:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session, session.begin():
            session.add(ProcessedK8sAppRow(key=key, processed_at=utcnow_naive()))
            await self._evict_if_needed(session)

    async def remove(self, namespace: str, name: str, completed_at: str) -> None:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session, session.begin():
            await session.execute(delete(ProcessedK8sAppRow).where(ProcessedK8sAppRow.key == key))
