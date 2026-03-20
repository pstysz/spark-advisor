"""Tests for storage poller."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from spark_advisor_storage_connector.connectors.protocol import EventLogRef
from spark_advisor_storage_connector.poller import StoragePoller


@pytest.fixture
def mock_connector() -> AsyncMock:
    """Mock storage connector."""
    return AsyncMock()


@pytest.fixture
def mock_broker() -> AsyncMock:
    """Mock NATS broker."""
    return AsyncMock()


@pytest.fixture
def mock_store() -> AsyncMock:
    """Mock polling store."""
    return AsyncMock()


@pytest.fixture
def storage_poller(
    mock_connector: AsyncMock,
    mock_broker: AsyncMock,
    mock_store: AsyncMock
) -> StoragePoller:
    """Storage poller instance."""
    return StoragePoller(
        connector=mock_connector,
        broker=mock_broker,
        publish_subject="storage.fetch",
        store=mock_store
    )


@pytest.fixture
def sample_refs() -> list[EventLogRef]:
    """Sample event log references."""
    return [
        EventLogRef(
            path="/spark-events/app-123.json",
            name="app-123.json",
            size=1024,
            modified_at=datetime(2024, 3, 9, 18, 40, 0, tzinfo=UTC)
        ),
        EventLogRef(
            path="/spark-events/app-456.json",
            name="app-456.json",
            size=512,
            modified_at=datetime(2024, 3, 9, 18, 41, 0, tzinfo=UTC)
        )
    ]


@pytest.mark.asyncio
async def test_poll_publishes_new_logs(
    storage_poller: StoragePoller,
    mock_connector: AsyncMock,
    mock_store: AsyncMock,
    sample_refs: list[EventLogRef],
    event_log_bytes: bytes
) -> None:
    """Test that polling publishes new logs."""
    # Mock backend returns 2 refs
    mock_connector.list_event_logs.return_value = sample_refs
    mock_connector.read_event_log.return_value = event_log_bytes

    # Mock store has 0 known logs (all are new)
    mock_store.filter_new_and_mark.return_value = [ref.path for ref in sample_refs]

    # Mock broker publish
    storage_poller._broker.publish = AsyncMock()

    result = await storage_poller.poll()

    # Verify 2 logs were published
    assert result == 2

    # Verify store was called with all paths
    all_paths = [ref.path for ref in sample_refs]
    mock_store.filter_new_and_mark.assert_called_once_with(all_paths)

    # Verify publish was called twice
    assert storage_poller._broker.publish.call_count == 2


@pytest.mark.asyncio
async def test_poll_skips_known_logs(
    storage_poller: StoragePoller,
    mock_connector: AsyncMock,
    mock_store: AsyncMock,
    sample_refs: list[EventLogRef]
) -> None:
    """Test that polling skips known logs."""
    # Mock backend returns 2 refs
    mock_connector.list_event_logs.return_value = sample_refs

    # Mock store knows both logs (none are new)
    mock_store.filter_new_and_mark.return_value = []

    result = await storage_poller.poll()

    # Verify 0 logs were published
    assert result == 0

    # Verify publish was not called
    storage_poller._broker.publish.assert_not_called()


@pytest.mark.asyncio
async def test_poll_handles_fetch_error(
    storage_poller: StoragePoller,
    mock_connector: AsyncMock,
    mock_store: AsyncMock,
    sample_refs: list[EventLogRef]
) -> None:
    """Test that polling handles fetch errors gracefully."""
    # Mock backend returns 1 ref
    mock_connector.list_event_logs.return_value = [sample_refs[0]]

    # Mock store returns 1 new path
    mock_store.filter_new_and_mark.return_value = [sample_refs[0].path]

    # Mock backend read raises exception
    mock_connector.read_event_log.side_effect = Exception("Network error")

    result = await storage_poller.poll()

    # Verify 0 logs were published due to error
    assert result == 0

    # Verify the failed path was removed from store
    mock_store.remove.assert_called_once_with(sample_refs[0].path)


@pytest.mark.asyncio
async def test_poll_no_new_logs(
    storage_poller: StoragePoller,
    mock_connector: AsyncMock,
    mock_store: AsyncMock
) -> None:
    """Test polling when no logs are found."""
    # Mock backend returns empty list
    mock_connector.list_event_logs.return_value = []

    # Mock store returns empty list (no new paths)
    mock_store.filter_new_and_mark.return_value = []

    result = await storage_poller.poll()

    # Verify 0 logs were published
    assert result == 0

    # Verify store was called with empty list
    mock_store.filter_new_and_mark.assert_called_once_with([])

    # Verify publish was not called
    storage_poller._broker.publish.assert_not_called()
