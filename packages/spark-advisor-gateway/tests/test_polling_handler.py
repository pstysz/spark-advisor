from __future__ import annotations

from unittest.mock import MagicMock

import orjson
import pytest

from spark_advisor_gateway.app import handle_polling_message
from spark_advisor_gateway.config import GatewaySettings
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.models import TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.model import AnalysisMode, AnalysisResult, JobAnalysis
from spark_advisor_models.testing import make_job


def make_job_result(job: JobAnalysis) -> AnalysisResult:
    return AnalysisResult(app_id=job.app_id, job=job, rule_results=[])


async def _setup() -> tuple[TaskManager, GatewaySettings]:
    store = TaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()
    manager = TaskManager(store)
    settings = GatewaySettings()
    return manager, settings


def _make_nats_msg(data: bytes) -> MagicMock:
    msg = MagicMock()
    msg.data = data
    return msg


@pytest.mark.asyncio
async def test_handle_polling_message_creates_task() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)

    tasks, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 1
    assert tasks[0].app_id == job.app_id
    assert tasks[0].status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_handle_polling_message_calls_submit_with_job() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)

    executor.submit_with_job.assert_called_once()
    call_args = executor.submit_with_job.call_args
    assert call_args.args[1].app_id == job.app_id
    assert call_args.args[2] == settings.nats.polling_analysis_mode


@pytest.mark.asyncio
async def test_handle_polling_message_uses_configured_mode() -> None:
    from spark_advisor_gateway.config import GatewayNatsSettings

    manager, settings = await _setup()
    settings.nats = GatewayNatsSettings(polling_analysis_mode=AnalysisMode.AGENT)
    executor = MagicMock()
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)

    call_args = executor.submit_with_job.call_args
    assert call_args.args[2] == AnalysisMode.AGENT


@pytest.mark.asyncio
async def test_handle_polling_message_invalid_data_does_not_create_task() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    msg = _make_nats_msg(b"not valid json at all")

    await handle_polling_message(msg, manager, executor, settings)

    _, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 0
    executor.submit_with_job.assert_not_called()


@pytest.mark.asyncio
async def test_handle_polling_message_incomplete_job_does_not_create_task() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    msg = _make_nats_msg(orjson.dumps({"app_id": "test", "missing_fields": True}))

    await handle_polling_message(msg, manager, executor, settings)

    _, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 0
    executor.submit_with_job.assert_not_called()


@pytest.mark.asyncio
async def test_handle_polling_message_skips_when_active_task_exists() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)
    assert executor.submit_with_job.call_count == 1

    await handle_polling_message(msg, manager, executor, settings)
    assert executor.submit_with_job.call_count == 1

    _, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 1


@pytest.mark.asyncio
async def test_handle_polling_message_creates_new_after_completed() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)
    tasks, _ = await manager.list_filtered(limit=10, offset=0)
    await manager.mark_completed(tasks[0].task_id, make_job_result(job))

    await handle_polling_message(msg, manager, executor, settings)
    assert executor.submit_with_job.call_count == 2

    _, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 2


@pytest.mark.asyncio
async def test_handle_polling_message_handles_submit_with_job_exception() -> None:
    manager, settings = await _setup()
    executor = MagicMock()
    executor.submit_with_job.side_effect = RuntimeError("Task submission failed")
    job = make_job()
    msg = _make_nats_msg(orjson.dumps(job.model_dump(mode="json")))

    await handle_polling_message(msg, manager, executor, settings)

    tasks, total = await manager.list_filtered(limit=10, offset=0)
    assert total == 1
    assert tasks[0].app_id == job.app_id
    assert tasks[0].status == TaskStatus.PENDING
    executor.submit_with_job.assert_called_once()
