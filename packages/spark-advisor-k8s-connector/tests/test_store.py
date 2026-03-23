import pytest
import pytest_asyncio

from spark_advisor_k8s_connector.store import K8sPollingStore


@pytest_asyncio.fixture
async def store() -> K8sPollingStore:
    s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:")
    await s.init()
    yield s  # type: ignore[misc]
    await s.close()


class TestK8sPollingStore:
    @pytest.mark.asyncio
    async def test_is_processed_returns_false_for_new(self, store: K8sPollingStore) -> None:
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_mark_processed_and_check(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_different_completed_at_not_processed(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T11:00:00")

    @pytest.mark.asyncio
    async def test_same_name_different_namespace(self, store: K8sPollingStore) -> None:
        await store.mark_processed("ns1", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("ns2", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_remove(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        await store.remove("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_eviction(self) -> None:
        s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:", max_size=3)
        await s.init()
        for i in range(5):
            await s.mark_processed("ns", f"job-{i}", f"2026-03-21T10:0{i}:00")
        assert not await s.is_processed("ns", "job-0", "2026-03-21T10:00:00")
        assert not await s.is_processed("ns", "job-1", "2026-03-21T10:01:00")
        assert await s.is_processed("ns", "job-4", "2026-03-21T10:04:00")
        await s.close()
