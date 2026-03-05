from unittest.mock import MagicMock

import orjson
import pytest
from faststream.nats import TestNatsBroker

from spark_advisor_hs_connector.app import app, broker
from spark_advisor_hs_connector.history_server_client import HistoryServerClient
from spark_advisor_hs_connector.model.output import ApplicationSummary, Attempt
from spark_advisor_models.model import JobAnalysis

ENVIRONMENT = {
    "sparkProperties": [["spark.executor.memory", "4g"]],
}

APP_INFO = {
    "id": "app-001",
    "name": "TestJob",
    "attempts": [{"attemptId": "1", "duration": 300000, "appSparkVersion": "3.5.0"}],
}

STAGES = [
    {
        "stageId": 0,
        "attemptId": 0,
        "name": "Stage 0",
        "numTasks": 10,
        "executorRunTime": 5000,
        "jvmGcTime": 100,
        "shuffleReadBytes": 0,
        "shuffleWriteBytes": 0,
        "diskBytesSpilled": 0,
        "memoryBytesSpilled": 0,
        "numFailedTasks": 0,
        "inputBytes": 0,
        "outputBytes": 0,
    }
]

TASK_SUMMARY = {"executorRunTime": [100, 200, 300, 400, 500]}

EXECUTORS = [
    {"id": "driver", "maxMemory": 1073741824},
    {"id": "1", "maxMemory": 4294967296},
]


def _mock_hs_client() -> MagicMock:
    mock = MagicMock(spec=HistoryServerClient)
    mock.get_app_info.return_value = APP_INFO
    mock.get_environment.return_value = ENVIRONMENT
    mock.get_stages.return_value = STAGES
    mock.get_task_summary.return_value = TASK_SUMMARY
    mock.get_executors.return_value = EXECUTORS
    return mock


@pytest.mark.asyncio
async def test_fetch_job_returns_job_analysis() -> None:
    hs_client = _mock_hs_client()

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("hs_client", hs_client)
        result = await br.request(
            {"app_id": "app-001"},
            subject="fetch.job",
            timeout=10.0,
        )
        parsed = JobAnalysis.model_validate_json(result.body)
        assert parsed.app_id == "app-001"
        assert parsed.app_name == "TestJob"
        assert len(parsed.stages) == 1
        hs_client.get_app_info.assert_called_once_with("app-001")


@pytest.mark.asyncio
async def test_list_applications_returns_list() -> None:
    hs_client = _mock_hs_client()
    hs_client.list_applications.return_value = [
        ApplicationSummary(
            id="app-001",
            name="TestJob",
            attempts=[Attempt(attempt_id="1", duration=300000, completed=True, app_spark_version="3.5.0")],
        ),
        ApplicationSummary(id="app-002", name="AnotherJob"),
    ]

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("hs_client", hs_client)
        result = await br.request(
            {"limit": 10},
            subject="list.applications",
            timeout=10.0,
        )
        parsed = orjson.loads(result.body)
        assert len(parsed) == 2
        assert parsed[0]["id"] == "app-001"
        assert parsed[1]["id"] == "app-002"
        hs_client.list_applications.assert_called_once_with(limit=10)


@pytest.mark.asyncio
async def test_list_applications_returns_error_on_failure() -> None:
    hs_client = _mock_hs_client()
    hs_client.list_applications.side_effect = Exception("Connection refused")

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("hs_client", hs_client)
        result = await br.request(
            {},
            subject="list.applications",
            timeout=10.0,
        )
        parsed = orjson.loads(result.body)
        assert "error" in parsed
        assert "Connection refused" in parsed["error"]
