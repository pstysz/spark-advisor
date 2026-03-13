import json

import httpx
import respx
from typer.testing import CliRunner

from spark_advisor_cli.app import app

runner = CliRunner()

HS_URL = "http://spark-hs:18080"
HS_BASE = f"{HS_URL}/api/v1"
APP_ID = "app-001"
APP_PATH = f"/applications/{APP_ID}"

APP_INFO = {
    "id": APP_ID,
    "name": "TestETL",
    "attempts": [
        {
            "startTime": "2026-01-01T00:00:00.000GMT",
            "endTime": "2026-01-01T00:05:00.000GMT",
            "duration": 300000,
            "sparkUser": "user",
            "completed": True,
        }
    ],
}

ENVIRONMENT = {
    "sparkProperties": [
        ["spark.executor.memory", "4g"],
        ["spark.executor.cores", "4"],
        ["spark.sql.shuffle.partitions", "200"],
    ],
    "runtime": {"sparkVersion": "3.5.0"},
}

STAGES = [
    {
        "stageId": 0,
        "attemptId": 0,
        "name": "Stage 0",
        "numTasks": 100,
        "executorRunTime": 50000,
        "jvmGcTime": 2000,
        "inputBytes": 1_000_000,
        "outputBytes": 500_000,
        "shuffleReadBytes": 200_000,
        "shuffleWriteBytes": 100_000,
        "diskBytesSpilled": 0,
        "memoryBytesSpilled": 0,
        "numFailedTasks": 0,
    }
]

TASK_SUMMARY = {
    "executorRunTime": [100, 300, 500, 700, 900],
}

EXECUTORS = [
    {"id": "driver", "maxMemory": 1073741824},
    {
        "id": "1",
        "maxMemory": 4294967296,
        "peakMemoryMetrics": {"JVMHeapMemory": 2147483648},
    },
]


def _mock_all_hs_endpoints() -> None:
    respx.get(f"{HS_BASE}{APP_PATH}").respond(json=APP_INFO)
    respx.get(f"{HS_BASE}{APP_PATH}/environment").respond(json=ENVIRONMENT)
    respx.get(f"{HS_BASE}{APP_PATH}/stages").respond(json=STAGES)
    respx.get(f"{HS_BASE}{APP_PATH}/stages/0/0/taskSummary").respond(json=TASK_SUMMARY)
    respx.get(f"{HS_BASE}{APP_PATH}/executors").respond(json=EXECUTORS)


class TestAnalyzeHistoryServer:
    @respx.mock
    def test_produces_result(self) -> None:
        _mock_all_hs_endpoints()

        result = runner.invoke(app, ["analyze", APP_ID, "-hs", HS_URL, "--mode", "static"])

        assert result.exit_code == 0
        assert "Spark Job Analysis" in result.output
        assert APP_ID in result.output

    @respx.mock
    def test_json_format(self) -> None:
        _mock_all_hs_endpoints()

        result = runner.invoke(app, ["analyze", APP_ID, "-hs", HS_URL, "--mode", "static", "-f", "json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["app_id"] == APP_ID
        assert "rule_results" in data
        assert "job" in data

    @respx.mock
    def test_verbose_shows_stage_breakdown(self) -> None:
        _mock_all_hs_endpoints()

        result = runner.invoke(app, ["analyze", APP_ID, "-hs", HS_URL, "--mode", "static", "--verbose"])

        assert result.exit_code == 0
        assert "Stage Breakdown" in result.output

    @respx.mock
    def test_404_shows_error(self) -> None:
        respx.get(f"{HS_BASE}{APP_PATH}").respond(status_code=404)

        result = runner.invoke(app, ["analyze", APP_ID, "-hs", HS_URL, "--mode", "static"])

        assert result.exit_code == 1
        assert "Error" in result.output

    @respx.mock
    def test_connection_error(self) -> None:
        respx.get(f"{HS_BASE}{APP_PATH}").mock(side_effect=httpx.ConnectError("Connection refused"))

        result = runner.invoke(app, ["analyze", APP_ID, "-hs", HS_URL, "--mode", "static"])

        assert result.exit_code == 1
        assert "Error" in result.output

    @respx.mock
    def test_source_not_treated_as_file_path(self) -> None:
        _mock_all_hs_endpoints()

        result = runner.invoke(app, ["analyze", "nonexistent-app-id", "-hs", HS_URL, "--mode", "static"])

        assert result.exit_code == 0 or "Error fetching" in result.output
