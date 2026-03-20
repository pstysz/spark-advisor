"""Tests for GCS connector."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from spark_advisor_storage_connector.config import GcsSettings
from spark_advisor_storage_connector.connectors.gcs import GcsConnector
from spark_advisor_storage_connector.connectors.protocol import EventLogRef


@pytest.fixture
def gcs_settings() -> GcsSettings:
    """GCS settings for testing."""
    return GcsSettings(
        bucket="test-bucket",
        prefix="spark-events/"
    )


@pytest.fixture
def gcs_backend(gcs_settings: GcsSettings) -> GcsConnector:
    """GCS connector instance."""
    return GcsConnector(gcs_settings)


@pytest.fixture
def mock_gcs_objects() -> dict[str, Any]:
    """Mock GCS list_objects response."""
    return {
        "items": [
            {
                "name": "spark-events/app-123.json",
                "size": "1024",
                "updated": "2024-03-09T18:40:00.000Z"
            },
            {
                "name": "spark-events/app-456.json.gz",
                "size": "512",
                "updated": "2024-03-09T18:41:00.000Z"
            },
            {
                "name": "spark-events/",  # Directory-like object (empty filename)
                "size": "0",
                "updated": "2024-03-09T18:00:00.000Z"
            }
        ]
    }


@pytest.mark.asyncio
async def test_list_event_logs(
    gcs_backend: GcsConnector,
    mock_gcs_objects: dict[str, Any]
) -> None:
    """Test listing event logs from GCS."""
    # Mock the GCS Storage client
    mock_storage = AsyncMock()
    mock_storage.list_objects.return_value = mock_gcs_objects

    gcs_backend._storage = mock_storage

    refs = await gcs_backend.list_event_logs()

    assert len(refs) == 2  # Directory-like object excluded

    app_123 = next(r for r in refs if r.name == "app-123.json")
    assert app_123.path == "spark-events/app-123.json"
    assert app_123.size == 1024
    assert app_123.modified_at == datetime(2024, 3, 9, 18, 40, 0, tzinfo=UTC)

    app_456 = next(r for r in refs if r.name == "app-456.json.gz")
    assert app_456.path == "spark-events/app-456.json.gz"
    assert app_456.size == 512
    assert app_456.modified_at == datetime(2024, 3, 9, 18, 41, 0, tzinfo=UTC)

    mock_storage.list_objects.assert_called_once_with("test-bucket", params={"prefix": "spark-events/"})


@pytest.mark.asyncio
async def test_list_event_logs_no_items(gcs_backend: GcsConnector) -> None:
    """Test listing when no items are returned."""
    mock_storage = AsyncMock()
    mock_storage.list_objects.return_value = {"items": []}

    gcs_backend._storage = mock_storage

    refs = await gcs_backend.list_event_logs()
    assert refs == []


@pytest.mark.asyncio
async def test_read_event_log(
    gcs_backend: GcsConnector,
    event_log_bytes: bytes
) -> None:
    """Test reading an event log from GCS."""
    ref = EventLogRef(
        path="spark-events/app-123.json",
        name="app-123.json",
        size=1024,
        modified_at=datetime.now(UTC)
    )

    # Mock the GCS Storage client
    mock_storage = AsyncMock()
    mock_storage.download.return_value = event_log_bytes

    gcs_backend._storage = mock_storage

    content = await gcs_backend.read_event_log(ref)

    assert content == event_log_bytes
    mock_storage.download.assert_called_once_with("test-bucket", "spark-events/app-123.json")


@pytest.mark.asyncio
async def test_close(gcs_backend: GcsConnector) -> None:
    """Test closing the GCS connector."""
    mock_storage = AsyncMock()
    gcs_backend._storage = mock_storage

    await gcs_backend.close()

    mock_storage.close.assert_called_once()
    assert gcs_backend._storage is None


@pytest.mark.asyncio
async def test_close_no_storage(gcs_backend: GcsConnector) -> None:
    """Test closing when no storage client is initialized."""
    await gcs_backend.close()
    # No assertion needed - just verify it doesn't raise
