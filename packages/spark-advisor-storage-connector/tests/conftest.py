"""Test configuration and fixtures for spark-advisor-storage-connector."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def event_log_bytes() -> bytes:
    """Valid event log content as bytes."""
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.0"},
        {"Event": "SparkListenerApplicationStart", "App ID": "app-1", "App Name": "Test", "Timestamp": 1000},
        {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"spark.executor.memory": "4g"}},
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": 0, "Stage Name": "read", "Accumulables": []},
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 5000},
    ]
    return "\n".join(json.dumps(e) for e in events).encode("utf-8")


@pytest.fixture
def temp_event_log_file(event_log_bytes: bytes) -> Path:
    """Create a temporary event log file."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        f.write(event_log_bytes)
        return Path(f.name)
