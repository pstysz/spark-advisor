from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from spark_advisor_k8s_connector.config import K8sConnectorSettings
from spark_advisor_k8s_connector.poller import K8sPoller
from spark_advisor_k8s_connector.store import K8sPollingStore
from tests.conftest import make_crd


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_broker() -> AsyncMock:
    broker = AsyncMock()
    broker.request.return_value = AsyncMock(body=b'{"app_id": "spark-abc123"}')
    return broker


@pytest_asyncio.fixture
async def store() -> K8sPollingStore:
    s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:")
    await s.init()
    yield s  # type: ignore[misc]
    await s.close()


@pytest.fixture
def settings() -> K8sConnectorSettings:
    return K8sConnectorSettings(
        namespaces=["spark-prod"],
        application_states=["COMPLETED"],
        max_age_days=7,
        default_event_log_dir="s3a://bucket/logs/",
        default_storage_type="s3",
    )


@pytest.fixture
def poller(
    mock_client: AsyncMock,
    mock_broker: AsyncMock,
    store: K8sPollingStore,
    settings: K8sConnectorSettings,
) -> K8sPoller:
    return K8sPoller(
        client=mock_client,
        broker=mock_broker,
        store=store,
        settings=settings,
    )


class TestPoll:
    @pytest.mark.asyncio
    async def test_processes_new_completed_app(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        count = await poller.poll()
        assert count == 1

    @pytest.mark.asyncio
    async def test_skips_already_processed(
        self, poller: K8sPoller, mock_client: AsyncMock, store: K8sPollingStore,
    ) -> None:
        await store.mark_processed("spark-prod", "my-spark-job", "2026-03-21T10:05:00+00:00")
        mock_client.list_applications.return_value = [make_crd()]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_running_state(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = [make_crd(state="RUNNING")]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_old_apps(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        old_time = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        mock_client.list_applications.return_value = [make_crd(termination_time=old_time)]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_no_event_log_dir(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir=None,
            default_storage_type="hdfs",
        )
        p = K8sPoller(client=mock_client, broker=poller._broker, store=poller._store, settings=settings)
        mock_client.list_applications.return_value = [make_crd(spark_conf={})]
        count = await p.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_empty_cluster(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = []
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_delegates_to_storage_connector(
        self, poller: K8sPoller, mock_client: AsyncMock, mock_broker: AsyncMock,
    ) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        await poller.poll()
        mock_broker.request.assert_called_once()
        call_args = mock_broker.request.call_args
        assert "storage.fetch.s3" in str(call_args)

    @pytest.mark.asyncio
    async def test_removes_from_store_on_delegate_failure(
        self, poller: K8sPoller, mock_client: AsyncMock, mock_broker: AsyncMock, store: K8sPollingStore,
    ) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        mock_broker.request.side_effect = Exception("storage down")
        count = await poller.poll()
        assert count == 0
        assert not await store.is_processed("spark-prod", "my-spark-job", "2026-03-21T10:05:00+00:00")
