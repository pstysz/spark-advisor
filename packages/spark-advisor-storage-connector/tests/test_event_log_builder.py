"""Tests for event log builder."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from spark_advisor_models.model import JobAnalysis
from spark_advisor_storage_connector.connectors.protocol import EventLogRef
from spark_advisor_storage_connector.event_log_builder import fetch_and_parse_event_log


@pytest.fixture
def event_log_ref() -> EventLogRef:
    """Event log reference for testing."""
    return EventLogRef(
        path="/spark-events/app-123.json",
        name="app-123.json",
        size=1024,
        modified_at=datetime.now(UTC)
    )


@pytest.fixture
def mock_backend() -> AsyncMock:
    """Mock storage backend."""
    return AsyncMock()


@pytest.mark.asyncio
async def test_fetch_and_parse_event_log(
    mock_backend: AsyncMock,
    event_log_ref: EventLogRef,
    event_log_bytes: bytes
) -> None:
    """Test fetching and parsing an event log."""
    # Configure mock backend
    mock_backend.read_event_log.return_value = event_log_bytes

    # Fetch and parse
    result = await fetch_and_parse_event_log(mock_backend, event_log_ref)

    # Verify result
    assert isinstance(result, JobAnalysis)
    assert result.app_id == "app-1"
    assert result.app_name == "Test"

    # Verify backend was called
    mock_backend.read_event_log.assert_called_once_with(event_log_ref)


@pytest.mark.asyncio
async def test_fetch_and_parse_compressed_log(
    mock_backend: AsyncMock,
    event_log_bytes: bytes
) -> None:
    """Test fetching and parsing a compressed event log."""
    import gzip

    # Create a compressed event log reference
    ref = EventLogRef(
        path="/spark-events/app-123.json.gz",
        name="app-123.json.gz",
        size=512,
        modified_at=datetime.now(UTC)
    )

    # Configure mock backend with compressed content
    compressed_bytes = gzip.compress(event_log_bytes)
    mock_backend.read_event_log.return_value = compressed_bytes

    # Fetch and parse
    result = await fetch_and_parse_event_log(mock_backend, ref)

    # Verify result
    assert isinstance(result, JobAnalysis)
    assert result.app_id == "app-1"
    assert result.app_name == "Test"
