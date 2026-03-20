"""Tests for HDFS connector."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import respx

from spark_advisor_storage_connector.config import HdfsSettings
from spark_advisor_storage_connector.connectors.hdfs import HdfsConnector
from spark_advisor_storage_connector.connectors.protocol import EventLogRef


@pytest.fixture
def hdfs_settings() -> HdfsSettings:
    """HDFS settings for testing."""
    return HdfsSettings(
        namenode_url="http://localhost:9870",
        event_log_dir="/spark-events",
    )


@pytest.fixture
def hdfs_backend(hdfs_settings: HdfsSettings) -> HdfsConnector:
    """HDFS connector instance."""
    return HdfsConnector(hdfs_settings)


@pytest.fixture
def webhdfs_liststatus_response() -> dict[str, Any]:
    """WebHDFS LISTSTATUS response with files and directories."""
    return {
        "FileStatuses": {
            "FileStatus": [
                {
                    "pathSuffix": "app-123.json",
                    "type": "FILE",
                    "length": 1024,
                    "modificationTime": 1710000000000
                },
                {
                    "pathSuffix": "app-456.json.gz",
                    "type": "FILE",
                    "length": 512,
                    "modificationTime": 1710001000000
                },
                {
                    "pathSuffix": "subdir",
                    "type": "DIRECTORY",
                    "length": 0,
                    "modificationTime": 1710000000000
                }
            ]
        }
    }


@pytest.mark.asyncio
@respx.mock
async def test_list_event_logs(
    hdfs_backend: HdfsConnector,
    webhdfs_liststatus_response: dict[str, Any]
) -> None:
    """Test listing event logs from HDFS."""
    # Mock the WebHDFS API
    respx.get("http://localhost:9870/webhdfs/v1/spark-events?op=LISTSTATUS").mock(
        return_value=httpx.Response(200, json=webhdfs_liststatus_response)
    )

    refs = await hdfs_backend.list_event_logs()

    assert len(refs) == 2  # Only files, not directories

    app_123 = next(r for r in refs if r.name == "app-123.json")
    assert app_123.path == "/spark-events/app-123.json"
    assert app_123.size == 1024
    assert app_123.modified_at == datetime(2024, 3, 9, 16, 0, 0, tzinfo=UTC)

    app_456 = next(r for r in refs if r.name == "app-456.json.gz")
    assert app_456.path == "/spark-events/app-456.json.gz"
    assert app_456.size == 512


@pytest.mark.asyncio
@respx.mock
async def test_list_event_logs_skips_directories(
    hdfs_backend: HdfsConnector
) -> None:
    """Test that directories are skipped when listing event logs."""
    response = {
        "FileStatuses": {
            "FileStatus": [
                {
                    "pathSuffix": "subdir1",
                    "type": "DIRECTORY",
                    "length": 0,
                    "modificationTime": 1710000000000
                },
                {
                    "pathSuffix": "subdir2",
                    "type": "DIRECTORY",
                    "length": 0,
                    "modificationTime": 1710001000000
                }
            ]
        }
    }

    respx.get("http://localhost:9870/webhdfs/v1/spark-events?op=LISTSTATUS").mock(
        return_value=httpx.Response(200, json=response)
    )

    refs = await hdfs_backend.list_event_logs()
    assert refs == []


@pytest.mark.asyncio
@respx.mock
async def test_read_event_log(
    hdfs_backend: HdfsConnector,
    event_log_bytes: bytes
) -> None:
    """Test reading an event log from HDFS."""
    ref = EventLogRef(
        path="/spark-events/app-123.json",
        name="app-123.json",
        size=1024,
        modified_at=datetime.now(UTC)
    )

    # Mock the WebHDFS OPEN operation
    respx.get("http://localhost:9870/webhdfs/v1/spark-events/app-123.json?op=OPEN").mock(
        return_value=httpx.Response(200, content=event_log_bytes)
    )

    content = await hdfs_backend.read_event_log(ref)
    assert content == event_log_bytes


@pytest.mark.asyncio
async def test_close(hdfs_backend: HdfsConnector) -> None:
    """Test closing the HDFS connector."""
    await hdfs_backend.close()
    # No assertion needed - just verify it doesn't raise
