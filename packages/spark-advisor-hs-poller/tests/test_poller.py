from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

from spark_advisor_hs_poller.model.output import ApplicationSummary, Attempt
from spark_advisor_hs_poller.poller import HistoryServerPoller
from spark_advisor_hs_poller.pooling_state import PollingState

if TYPE_CHECKING:
    from spark_advisor_shared.model.events import KafkaEnvelope


def _make_app_summary(app_id: str) -> ApplicationSummary:
    return ApplicationSummary(
        id=app_id,
        name=f"Job-{app_id}",
        attempts=[
            Attempt(
                attemptId="1",
                startTime="2026-01-01T00:00:00",
                endTime="2026-01-01T00:05:00",
                lastUpdated="2026-01-01T00:05:00",
                duration=300000,
                sparkUser="user",
                completed=True,
                appSparkVersion="3.5.0",
                logPath=f"hdfs:///logs/{app_id}",
                startTimeEpoch=1735689600000,
                endTimeEpoch=1735689900000,
                lastUpdatedEpoch=1735689900000,
            )
        ],
        driverHost="10.0.0.1",
    )


def _app_info(app_id: str) -> dict[str, Any]:
    return {
        "id": app_id,
        "name": f"Job-{app_id}",
        "attempts": [{"attemptId": "1", "duration": 300000}],
    }


ENVIRONMENT: dict[str, Any] = {
    "sparkProperties": [["spark.executor.memory", "4g"]],
    "runtime": {"sparkVersion": "3.5.0"},
}

STAGES: list[dict[str, Any]] = [
    {
        "stageId": 0,
        "attemptId": 0,
        "name": "Stage 0",
        "numTasks": 100,
        "executorRunTime": 100_000,
        "jvmGcTime": 5000,
        "shuffleReadBytes": 0,
        "shuffleWriteBytes": 0,
        "diskBytesSpilled": 0,
        "memoryBytesSpilled": 0,
        "numFailedTasks": 0,
        "inputBytes": 0,
        "outputBytes": 0,
    }
]

TASK_SUMMARY: dict[str, Any] = {
    "executorRunTime": [500, 800, 1000, 1200, 2000],
}

EXECUTORS: list[dict[str, Any]] = [
    {"id": "driver", "maxMemory": 1073741824},
    {"id": "1", "maxMemory": 4294967296, "peakMemoryMetrics": {"JVMHeapMemory": 2147483648}},
]


def _configure_hs_client(mock: MagicMock, app_ids: list[str]) -> None:
    mock.list_applications.return_value = [_make_app_summary(aid) for aid in app_ids]
    mock.get_app_info.side_effect = lambda aid: _app_info(aid)
    mock.get_environment.return_value = ENVIRONMENT
    mock.get_stages.return_value = STAGES
    mock.get_task_summary.return_value = TASK_SUMMARY
    mock.get_executors.return_value = EXECUTORS


class TestHistoryServerPoller:
    def test_poll_publishes_new_apps(self) -> None:
        hs_client = MagicMock()
        _configure_hs_client(hs_client, ["app-001", "app-002"])

        sink = MagicMock()
        state = PollingState()

        poller = HistoryServerPoller(hs_client=hs_client, sink=sink, pool_state=state, batch_size=10)
        published = poller.poll()

        assert published == 2
        assert sink.send.call_count == 2
        assert state.is_processed("app-001")
        assert state.is_processed("app-002")

        envelope: KafkaEnvelope = sink.send.call_args_list[0][0][0]
        assert envelope.payload["app_id"] == "app-001"
        assert envelope.metadata.source == "hs-poller"

    def test_poll_skips_already_processed(self) -> None:
        hs_client = MagicMock()
        _configure_hs_client(hs_client, ["app-001", "app-002"])

        sink = MagicMock()
        state = PollingState()
        state.mark_processed("app-001")

        poller = HistoryServerPoller(hs_client=hs_client, sink=sink, pool_state=state)
        published = poller.poll()

        assert published == 1
        hs_client.get_app_info.assert_called_once_with("app-002")

    def test_poll_returns_zero_when_no_new(self) -> None:
        hs_client = MagicMock()
        hs_client.list_applications.return_value = []

        sink = MagicMock()
        state = PollingState()

        poller = HistoryServerPoller(hs_client=hs_client, sink=sink, pool_state=state)
        published = poller.poll()

        assert published == 0
        sink.send.assert_not_called()

    def test_poll_continues_on_single_fetch_failure(self) -> None:
        hs_client = MagicMock()
        hs_client.list_applications.return_value = [
            _make_app_summary("app-fail"),
            _make_app_summary("app-ok"),
        ]
        hs_client.get_app_info.side_effect = [RuntimeError("HS down"), _app_info("app-ok")]
        hs_client.get_environment.return_value = ENVIRONMENT
        hs_client.get_stages.return_value = STAGES
        hs_client.get_task_summary.return_value = TASK_SUMMARY
        hs_client.get_executors.return_value = EXECUTORS

        sink = MagicMock()
        state = PollingState()

        poller = HistoryServerPoller(hs_client=hs_client, sink=sink, pool_state=state)
        published = poller.poll()

        assert published == 1
        assert not state.is_processed("app-fail")
        assert state.is_processed("app-ok")

    def test_resolve_base_path_with_attempt_id(self) -> None:
        app_info: dict[str, Any] = {
            "attempts": [{"attemptId": "2", "duration": 100}],
        }
        assert HistoryServerPoller._resolve_base_path("app-001", app_info) == "/applications/app-001/2"

    def test_resolve_base_path_without_attempt_id(self) -> None:
        app_info: dict[str, Any] = {
            "attempts": [{"duration": 100}],
        }
        assert HistoryServerPoller._resolve_base_path("app-001", app_info) == "/applications/app-001"

    def test_resolve_base_path_no_attempts(self) -> None:
        app_info: dict[str, Any] = {"attempts": []}
        assert HistoryServerPoller._resolve_base_path("app-001", app_info) == "/applications/app-001"

    def test_deduplicate_stages_keeps_latest_attempt(self) -> None:
        stages: list[dict[str, Any]] = [
            {"stageId": 0, "attemptId": 0, "name": "Stage 0 attempt 0"},
            {"stageId": 0, "attemptId": 1, "name": "Stage 0 attempt 1"},
            {"stageId": 1, "attemptId": 0, "name": "Stage 1"},
        ]
        result = HistoryServerPoller._deduplicate_stages(stages)
        assert len(result) == 2
        stage_0 = next(s for s in result if s["stageId"] == 0)
        assert stage_0["attemptId"] == 1
        assert stage_0["name"] == "Stage 0 attempt 1"

    def test_deduplicate_stages_handles_reverse_order(self) -> None:
        stages: list[dict[str, Any]] = [
            {"stageId": 0, "attemptId": 2, "name": "attempt 2"},
            {"stageId": 0, "attemptId": 0, "name": "attempt 0"},
            {"stageId": 0, "attemptId": 1, "name": "attempt 1"},
        ]
        result = HistoryServerPoller._deduplicate_stages(stages)
        assert len(result) == 1
        assert result[0]["attemptId"] == 2

    def test_deduplicate_stages_no_duplicates(self) -> None:
        stages: list[dict[str, Any]] = [
            {"stageId": 0, "attemptId": 0},
            {"stageId": 1, "attemptId": 0},
        ]
        result = HistoryServerPoller._deduplicate_stages(stages)
        assert len(result) == 2
