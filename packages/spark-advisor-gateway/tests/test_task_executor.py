from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "spark-advisor-models" / "tests"))

from factories import make_job

from spark_advisor_gateway.config import GatewaySettings
from spark_advisor_gateway.task.executor import TaskExecutor
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.models import TaskStatus
from spark_advisor_gateway.task.store import InMemoryTaskStore
from spark_advisor_models.model import AnalysisResult


def _make_reply(data: bytes) -> MagicMock:
    msg = MagicMock()
    msg.data = data
    return msg


def _setup() -> tuple[AsyncMock, TaskManager, TaskExecutor]:
    nc = AsyncMock()
    manager = TaskManager(InMemoryTaskStore())
    settings = GatewaySettings()
    executor = TaskExecutor(nc, manager, settings)
    return nc, manager, executor


@pytest.mark.asyncio
async def test_execute_success() -> None:
    nc, manager, executor = _setup()
    job = make_job()

    job_bytes = orjson.dumps(job.model_dump(mode="json"))
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)
    result_bytes = result.model_dump_json().encode()

    nc.request = AsyncMock(side_effect=[
        _make_reply(job_bytes),
        _make_reply(result_bytes),
    ])

    task = manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001")
    await asyncio.sleep(0.1)

    updated = manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.result is not None
    assert updated.result.app_id == "app-test-001"


@pytest.mark.asyncio
async def test_execute_marks_running() -> None:
    nc, manager, executor = _setup()

    started_event = asyncio.Event()

    async def slow_request(*_args: object, **_kwargs: object) -> MagicMock:
        started_event.set()
        await asyncio.sleep(10)
        return _make_reply(b"{}")

    nc.request = slow_request

    task = manager.create("app-slow")
    executor.submit(task.task_id, "app-slow")
    await started_event.wait()

    updated = manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_execute_marks_failed_on_error() -> None:
    nc, manager, executor = _setup()
    nc.request = AsyncMock(side_effect=TimeoutError("NATS timeout"))

    task = manager.create("app-fail")
    executor.submit(task.task_id, "app-fail")
    await asyncio.sleep(0.1)

    updated = manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.FAILED
    assert updated.error is not None
    assert "timeout" in updated.error.lower()


@pytest.mark.asyncio
async def test_execute_sends_correct_subjects() -> None:
    nc, manager, executor = _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(side_effect=[
        _make_reply(orjson.dumps(job.model_dump(mode="json"))),
        _make_reply(result.model_dump_json().encode()),
    ])

    task = manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001")
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[0].args[0] == "fetch.job"
    assert calls[1].args[0] == "analyze.request"
