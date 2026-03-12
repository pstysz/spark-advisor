from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from spark_advisor_hs_connector.history_server.poller import HistoryServerPoller
from spark_advisor_hs_connector.job_analysis_builder import deduplicate_stages, resolve_base_path
from spark_advisor_hs_connector.store import PollingStore
from spark_advisor_models.defaults import NATS_ANALYZE_REQUEST_SUBJECT
from spark_advisor_models.model import ApplicationSummary, Attempt

_DB_URL = "sqlite+aiosqlite:///:memory:"


def _make_app_summary(app_id: str) -> ApplicationSummary:
    return ApplicationSummary(
        id=app_id,
        name=f"Job-{app_id}",
        attempts=[
            Attempt(
                attempt_id="1",
                start_time="2026-01-01T00:00:00",
                end_time="2026-01-01T00:05:00",
                last_updated="2026-01-01T00:05:00",
                duration=300000,
                spark_user="user",
                completed=True,
                app_spark_version="3.5.0",
                log_path=f"hdfs:///logs/{app_id}",
                start_time_epoch=1735689600000,
                end_time_epoch=1735689900000,
                last_updated_epoch=1735689900000,
            )
        ],
        driver_host="10.0.0.1",
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


async def _make_state() -> PollingStore:
    state = PollingStore(_DB_URL)
    await state.init()
    return state


class TestHistoryServerPoller:
    @pytest.mark.asyncio
    async def test_poll_publishes_new_apps(self) -> None:
        hs_client = MagicMock()
        _configure_hs_client(hs_client, ["app-001", "app-002"])

        nats_broker = AsyncMock()
        state = await _make_state()

        poller = HistoryServerPoller(
            hs_client=hs_client,
            broker=nats_broker,
            publish_subject=NATS_ANALYZE_REQUEST_SUBJECT,
            store=state,
            batch_size=10,
        )
        published = await poller.poll()

        assert published == 2
        assert nats_broker.publish.call_count == 2
        assert await state.is_processed("app-001")
        assert await state.is_processed("app-002")

        payload = nats_broker.publish.call_args_list[0]
        assert payload[1]["subject"] == NATS_ANALYZE_REQUEST_SUBJECT

    @pytest.mark.asyncio
    async def test_poll_skips_already_processed(self) -> None:
        hs_client = MagicMock()
        _configure_hs_client(hs_client, ["app-001", "app-002"])

        nats_broker = AsyncMock()
        state = await _make_state()
        await state.mark_processed("app-001")

        poller = HistoryServerPoller(
            hs_client=hs_client,
            broker=nats_broker,
            publish_subject=NATS_ANALYZE_REQUEST_SUBJECT,
            store=state,
        )
        published = await poller.poll()

        assert published == 1
        hs_client.get_app_info.assert_called_once_with("app-002")

    @pytest.mark.asyncio
    async def test_poll_returns_zero_when_no_new(self) -> None:
        hs_client = MagicMock()
        hs_client.list_applications.return_value = []

        nats_broker = AsyncMock()
        state = await _make_state()

        poller = HistoryServerPoller(
            hs_client=hs_client,
            broker=nats_broker,
            publish_subject=NATS_ANALYZE_REQUEST_SUBJECT,
            store=state,
        )
        published = await poller.poll()

        assert published == 0
        nats_broker.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll_continues_on_single_fetch_failure(self) -> None:
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

        nats_broker = AsyncMock()
        state = await _make_state()

        poller = HistoryServerPoller(
            hs_client=hs_client,
            broker=nats_broker,
            publish_subject=NATS_ANALYZE_REQUEST_SUBJECT,
            store=state,
        )
        published = await poller.poll()

        assert published == 1
        assert not await state.is_processed("app-fail")
        assert await state.is_processed("app-ok")

    def test_resolve_base_path_with_attempt_id(self) -> None:
        app_info: dict[str, Any] = {
            "attempts": [{"attemptId": "2", "duration": 100}],
        }
        assert resolve_base_path("app-001", app_info) == "/applications/app-001/2"

    def test_resolve_base_path_without_attempt_id(self) -> None:
        app_info: dict[str, Any] = {
            "attempts": [{"duration": 100}],
        }
        assert resolve_base_path("app-001", app_info) == "/applications/app-001"

    def test_resolve_base_path_no_attempts(self) -> None:
        app_info: dict[str, Any] = {"attempts": []}
        assert resolve_base_path("app-001", app_info) == "/applications/app-001"

    def test_deduplicate_stages_keeps_latest_attempt(self) -> None:
        stages: list[dict[str, Any]] = [
            {"stageId": 0, "attemptId": 0, "name": "Stage 0 attempt 0"},
            {"stageId": 0, "attemptId": 1, "name": "Stage 0 attempt 1"},
            {"stageId": 1, "attemptId": 0, "name": "Stage 1"},
        ]
        result = deduplicate_stages(stages)
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
        result = deduplicate_stages(stages)
        assert len(result) == 1
        assert result[0]["attemptId"] == 2

    def test_deduplicate_stages_no_duplicates(self) -> None:
        stages: list[dict[str, Any]] = [
            {"stageId": 0, "attemptId": 0},
            {"stageId": 1, "attemptId": 0},
        ]
        result = deduplicate_stages(stages)
        assert len(result) == 2
