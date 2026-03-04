import json

import httpx
import respx
from typer.testing import CliRunner

from spark_advisor_cli.app import app

runner = CliRunner()

HS_URL = "http://spark-hs:18080"
HS_BASE = f"{HS_URL}/api/v1"

APPLICATIONS_LIST = [
    {
        "id": "app-001",
        "name": "TestETL",
        "attempts": [
            {
                "attemptId": "1",
                "startTime": "2026-01-01T00:00:00.000GMT",
                "endTime": "2026-01-01T00:05:00.000GMT",
                "duration": 300000,
                "sparkUser": "user",
                "completed": True,
                "appSparkVersion": "3.5.0",
            }
        ],
    },
    {
        "id": "app-002",
        "name": "DailyAgg",
        "attempts": [
            {
                "attemptId": "1",
                "duration": 0,
                "completed": False,
                "appSparkVersion": "3.4.1",
            }
        ],
    },
]


class TestScanCommand:
    @respx.mock
    def test_lists_applications(self) -> None:
        respx.get(f"{HS_BASE}/applications").respond(content=json.dumps(APPLICATIONS_LIST).encode())

        result = runner.invoke(app, ["scan", "-hs", HS_URL])

        assert result.exit_code == 0
        assert "app-001" in result.output
        assert "TestETL" in result.output
        assert "app-002" in result.output
        assert "DailyAgg" in result.output

    @respx.mock
    def test_shows_duration_and_status(self) -> None:
        respx.get(f"{HS_BASE}/applications").respond(content=json.dumps(APPLICATIONS_LIST).encode())

        result = runner.invoke(app, ["scan", "-hs", HS_URL])

        assert "5.0 min" in result.output
        assert "completed" in result.output
        assert "running" in result.output

    @respx.mock
    def test_empty_list(self) -> None:
        respx.get(f"{HS_BASE}/applications").respond(content=b"[]")

        result = runner.invoke(app, ["scan", "-hs", HS_URL])

        assert result.exit_code == 0
        assert "No applications found" in result.output

    def test_requires_history_server(self) -> None:
        result = runner.invoke(app, ["scan"])

        assert result.exit_code != 0

    @respx.mock
    def test_connection_error(self) -> None:
        respx.get(f"{HS_BASE}/applications").mock(side_effect=httpx.ConnectError("Connection refused"))

        result = runner.invoke(app, ["scan", "-hs", HS_URL])

        assert result.exit_code == 1
        assert "Error" in result.output

    @respx.mock
    def test_limit_parameter(self) -> None:
        respx.get(f"{HS_BASE}/applications").respond(content=json.dumps(APPLICATIONS_LIST[:1]).encode())

        result = runner.invoke(app, ["scan", "-hs", HS_URL, "--limit", "5"])

        assert result.exit_code == 0
        assert "app-001" in result.output
