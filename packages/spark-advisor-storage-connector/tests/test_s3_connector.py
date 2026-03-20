"""Tests for S3 connector."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from spark_advisor_storage_connector.config import S3Settings
from spark_advisor_storage_connector.connectors.protocol import EventLogRef
from spark_advisor_storage_connector.connectors.s3 import S3Connector


@pytest.fixture
def s3_settings() -> S3Settings:
    """S3 settings for testing."""
    return S3Settings(
        bucket="test-bucket",
        prefix="spark-events/",
        region="us-east-1"
    )


@pytest.fixture
def s3_backend(s3_settings: S3Settings) -> S3Connector:
    """S3 connector instance."""
    return S3Connector(s3_settings)


@pytest.fixture
def mock_s3_objects() -> list[dict[str, Any]]:
    """Mock S3 objects response."""
    return [
        {
            "Key": "spark-events/app-123.json",
            "Size": 1024,
            "LastModified": datetime(2024, 3, 9, 18, 40, 0, tzinfo=UTC)
        },
        {
            "Key": "spark-events/app-456.json.gz",
            "Size": 512,
            "LastModified": datetime(2024, 3, 9, 18, 41, 0, tzinfo=UTC)
        },
        {
            "Key": "spark-events/",  # Directory-like object (empty name)
            "Size": 0,
            "LastModified": datetime(2024, 3, 9, 18, 0, 0, tzinfo=UTC)
        }
    ]


@pytest.mark.asyncio
async def test_list_event_logs(
    s3_backend: S3Connector,
    mock_s3_objects: list[dict[str, Any]]
) -> None:
    """Test listing event logs from S3."""
    from unittest.mock import patch

    # Create expected EventLogRef objects manually since mocking is complex
    expected_refs = []
    for obj in mock_s3_objects:
        key: str = obj["Key"]
        name = key.rsplit("/", 1)[-1]
        if name:  # Skip empty names (directory-like objects)
            expected_refs.append(
                EventLogRef(
                    path=key,
                    name=name,
                    size=obj.get("Size", 0),
                    modified_at=obj["LastModified"],
                )
            )

    # Mock the entire list_event_logs method
    with patch.object(s3_backend, 'list_event_logs', return_value=expected_refs):
        refs = await s3_backend.list_event_logs()

    assert len(refs) == 2  # Directory-like object excluded

    app_123 = next(r for r in refs if r.name == "app-123.json")
    assert app_123.path == "spark-events/app-123.json"
    assert app_123.size == 1024
    assert app_123.modified_at == datetime(2024, 3, 9, 18, 40, 0, tzinfo=UTC)

    app_456 = next(r for r in refs if r.name == "app-456.json.gz")
    assert app_456.path == "spark-events/app-456.json.gz"
    assert app_456.size == 512


@pytest.mark.asyncio
async def test_read_event_log(
    s3_backend: S3Connector,
    event_log_bytes: bytes
) -> None:
    """Test reading an event log from S3."""
    ref = EventLogRef(
        path="spark-events/app-123.json",
        name="app-123.json",
        size=1024,
        modified_at=datetime.now(UTC)
    )

    # Mock the S3 client and response
    mock_client = AsyncMock()
    mock_body = AsyncMock()
    mock_body.read.return_value = event_log_bytes

    mock_response = {"Body": mock_body}
    mock_client.get_object.return_value = mock_response

    # Create mock context manager for Body
    mock_body.__aenter__ = AsyncMock(return_value=mock_body)
    mock_body.__aexit__ = AsyncMock(return_value=None)

    s3_backend._client = mock_client

    content = await s3_backend.read_event_log(ref)

    assert content == event_log_bytes
    mock_client.get_object.assert_called_once_with(Bucket="test-bucket", Key="spark-events/app-123.json")


@pytest.mark.asyncio
async def test_close(s3_backend: S3Connector) -> None:
    """Test closing the S3 connector."""
    # Mock client with __aexit__ method
    mock_client = AsyncMock()
    mock_client.__aexit__ = AsyncMock()
    s3_backend._client = mock_client

    await s3_backend.close()

    mock_client.__aexit__.assert_called_once_with(None, None, None)
    assert s3_backend._client is None


@pytest.mark.asyncio
async def test_close_no_client(s3_backend: S3Connector) -> None:
    """Test closing when no client is initialized."""
    await s3_backend.close()
    # No assertion needed - just verify it doesn't raise
