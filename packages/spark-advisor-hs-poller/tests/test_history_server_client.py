import json

import httpx
import pytest
import respx

from spark_advisor_hs_poller.history_server_client import HistoryServerClient

BASE = "http://spark-hs:18080/api/v1"

APP_INFO = {
    "id": "app-001",
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
    {
        "id": "2",
        "maxMemory": 4294967296,
        "peakMemoryMetrics": {"JVMHeapMemory": 1073741824},
    },
]

APPLICATIONS_LIST = [
    {
        "id": "app-001",
        "name": "TestETL",
        "attempts": [
            {
                "attemptId": "1",
                "startTime": "2026-01-01T00:00:00.000GMT",
                "endTime": "2026-01-01T00:05:00.000GMT",
                "lastUpdated": "2026-01-01T00:05:00.000GMT",
                "duration": 300000,
                "sparkUser": "user",
                "completed": True,
                "appSparkVersion": "3.5.0",
                "logPath": "hdfs:///spark-logs/app-001",
                "startTimeEpoch": 1735689600000,
                "endTimeEpoch": 1735689900000,
                "lastUpdatedEpoch": 1735689900000,
            }
        ],
        "driverHost": "10.0.0.1",
    },
]


class TestHistoryServerClientGetAppInfo:
    @respx.mock
    def test_returns_raw_dict(self) -> None:
        respx.get(f"{BASE}/applications/app-001").respond(json=APP_INFO)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_app_info("app-001")

        assert result["id"] == "app-001"
        assert result["name"] == "TestETL"
        assert result["attempts"][0]["duration"] == 300000

    @respx.mock
    def test_raises_on_404(self) -> None:
        respx.get(f"{BASE}/applications/bad-id").respond(status_code=404)
        with HistoryServerClient("http://spark-hs:18080") as client, pytest.raises(httpx.HTTPStatusError):
            client.get_app_info("bad-id")


class TestHistoryServerClientGetEnvironment:
    @respx.mock
    def test_returns_raw_dict(self) -> None:
        respx.get(f"{BASE}/applications/app-001/environment").respond(json=ENVIRONMENT)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_environment("/applications/app-001")

        assert len(result["sparkProperties"]) == 3
        assert result["runtime"]["sparkVersion"] == "3.5.0"

    @respx.mock
    def test_with_attempt_id(self) -> None:
        respx.get(f"{BASE}/applications/app-001/2/environment").respond(json=ENVIRONMENT)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_environment("/applications/app-001/2")

        assert result["runtime"]["sparkVersion"] == "3.5.0"


class TestHistoryServerClientGetStages:
    @respx.mock
    def test_returns_list(self) -> None:
        respx.get(f"{BASE}/applications/app-001/stages").respond(json=STAGES)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_stages("/applications/app-001")

        assert len(result) == 1
        assert result[0]["stageId"] == 0
        assert result[0]["numTasks"] == 100


class TestHistoryServerClientGetTaskSummary:
    @respx.mock
    def test_returns_raw_dict(self) -> None:
        respx.get(f"{BASE}/applications/app-001/stages/0/0/taskSummary").respond(json=TASK_SUMMARY)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_task_summary("/applications/app-001", stage_id=0, stage_attempt_id=0)

        assert result["executorRunTime"] == [100, 300, 500, 700, 900]

    @respx.mock
    def test_returns_empty_dict_on_404(self) -> None:
        respx.get(f"{BASE}/applications/app-001/stages/0/0/taskSummary").respond(status_code=404)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_task_summary("/applications/app-001", stage_id=0, stage_attempt_id=0)

        assert result == {}


class TestHistoryServerClientGetExecutors:
    @respx.mock
    def test_returns_list(self) -> None:
        respx.get(f"{BASE}/applications/app-001/executors").respond(json=EXECUTORS)
        with HistoryServerClient("http://spark-hs:18080") as client:
            result = client.get_executors("/applications/app-001")

        assert len(result) == 3
        assert result[0]["id"] == "driver"
        assert result[1]["id"] == "1"


class TestHistoryServerClientListApplications:
    @respx.mock
    def test_list_applications(self) -> None:
        respx.get(f"{BASE}/applications").respond(content=json.dumps(APPLICATIONS_LIST).encode())
        with HistoryServerClient("http://spark-hs:18080") as client:
            apps = client.list_applications(limit=10)

        assert len(apps) == 1
        assert apps[0].id == "app-001"
        assert apps[0].name == "TestETL"
        assert apps[0].attempts[0].attemptId == "1"

    @respx.mock
    def test_list_applications_with_null_attempt_id(self) -> None:
        app_no_attempt_id = [
            {
                "id": "app-002",
                "name": "ClientModeJob",
                "attempts": [
                    {
                        "attemptId": None,
                        "startTime": "2026-01-01T00:00:00.000GMT",
                        "duration": 60000,
                        "sparkUser": "user",
                        "completed": True,
                    }
                ],
            }
        ]
        respx.get(f"{BASE}/applications").respond(content=json.dumps(app_no_attempt_id).encode())
        with HistoryServerClient("http://spark-hs:18080") as client:
            apps = client.list_applications(limit=10)

        assert len(apps) == 1
        assert apps[0].attempts[0].attemptId is None

    @respx.mock
    def test_list_applications_empty(self) -> None:
        respx.get(f"{BASE}/applications").respond(content=b"[]")
        with HistoryServerClient("http://spark-hs:18080") as client:
            apps = client.list_applications()

        assert apps == []


class TestHistoryServerClientLifecycle:
    def test_requires_context_manager(self) -> None:
        client = HistoryServerClient("http://spark-hs:18080")
        with pytest.raises(RuntimeError, match="within 'with' block"):
            client.list_applications()

    def test_close_is_safe_when_not_opened(self) -> None:
        client = HistoryServerClient("http://spark-hs:18080")
        client.close()
