from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from spark_advisor_gateway.config import GatewaySettings
from spark_advisor_gateway.task.executor import TaskExecutor
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.models import TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.defaults import (
    NATS_ANALYSIS_RUN_AGENT_SUBJECT,
    NATS_ANALYSIS_RUN_SUBJECT,
    NATS_FETCH_JOB_SUBJECT,
)
from spark_advisor_models.model import AnalysisMode, AnalysisResult
from spark_advisor_models.testing import make_job


def _make_reply(data: bytes) -> MagicMock:
    msg = MagicMock()
    msg.data = data
    return msg


async def _setup() -> tuple[AsyncMock, TaskManager, TaskExecutor]:
    nc = AsyncMock()
    store = TaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()
    manager = TaskManager(store)
    settings = GatewaySettings()
    executor = TaskExecutor(nc, manager, settings)
    return nc, manager, executor


@pytest.mark.asyncio
async def test_execute_success() -> None:
    nc, manager, executor = await _setup()
    job = make_job()

    job_bytes = orjson.dumps(job.model_dump(mode="json"))
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)
    result_bytes = result.model_dump_json().encode()

    nc.request = AsyncMock(
        side_effect=[
            _make_reply(job_bytes),
            _make_reply(result_bytes),
        ]
    )

    task = await manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001")
    await asyncio.sleep(0.1)

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.result is not None
    assert updated.result.app_id == "app-test-001"


@pytest.mark.asyncio
async def test_execute_marks_running() -> None:
    nc, manager, executor = await _setup()

    started_event = asyncio.Event()

    async def slow_request(*_args: object, **_kwargs: object) -> MagicMock:
        started_event.set()
        await asyncio.sleep(10)
        return _make_reply(b"{}")

    nc.request = slow_request

    task = await manager.create("app-slow")
    executor.submit(task.task_id, "app-slow")
    await started_event.wait()

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_execute_marks_failed_on_error() -> None:
    nc, manager, executor = await _setup()
    nc.request = AsyncMock(side_effect=TimeoutError("NATS timeout"))

    task = await manager.create("app-fail")
    executor.submit(task.task_id, "app-fail")
    await asyncio.sleep(0.1)

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.FAILED
    assert updated.error is not None
    assert "timeout" in updated.error.lower()


@pytest.mark.asyncio
async def test_execute_sends_correct_subjects() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(
        side_effect=[
            _make_reply(orjson.dumps(job.model_dump(mode="json"))),
            _make_reply(result.model_dump_json().encode()),
        ]
    )

    task = await manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001")
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[0].args[0] == NATS_FETCH_JOB_SUBJECT
    assert calls[1].args[0] == NATS_ANALYSIS_RUN_SUBJECT


@pytest.mark.asyncio
async def test_execute_agent_mode_uses_agent_subject() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(
        side_effect=[
            _make_reply(orjson.dumps(job.model_dump(mode="json"))),
            _make_reply(result.model_dump_json().encode()),
        ]
    )

    task = await manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001", mode=AnalysisMode.AGENT)
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[0].args[0] == NATS_FETCH_JOB_SUBJECT
    assert calls[1].args[0] == NATS_ANALYSIS_RUN_AGENT_SUBJECT


@pytest.mark.asyncio
async def test_execute_agent_mode_uses_agent_timeout() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(
        side_effect=[
            _make_reply(orjson.dumps(job.model_dump(mode="json"))),
            _make_reply(result.model_dump_json().encode()),
        ]
    )

    task = await manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001", mode=AnalysisMode.AGENT)
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[1].kwargs["timeout"] == 300.0


@pytest.mark.asyncio
async def test_execute_standard_mode_default() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(
        side_effect=[
            _make_reply(orjson.dumps(job.model_dump(mode="json"))),
            _make_reply(result.model_dump_json().encode()),
        ]
    )

    task = await manager.create("app-test-001")
    executor.submit(task.task_id, "app-test-001")
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[1].args[0] == NATS_ANALYSIS_RUN_SUBJECT
    assert calls[1].kwargs["timeout"] == 120.0


@pytest.mark.asyncio
async def test_submit_with_job_success() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(return_value=_make_reply(result.model_dump_json().encode()))

    task = await manager.create(job.app_id)
    executor.submit_with_job(task.task_id, job)
    await asyncio.sleep(0.1)

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.result is not None
    assert updated.result.app_id == job.app_id

    assert nc.request.call_count == 1
    assert nc.request.call_args.args[0] == NATS_ANALYSIS_RUN_SUBJECT


@pytest.mark.asyncio
async def test_submit_with_job_skips_fetch() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(return_value=_make_reply(result.model_dump_json().encode()))

    task = await manager.create(job.app_id)
    executor.submit_with_job(task.task_id, job)
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert len(calls) == 1
    assert calls[0].args[0] != NATS_FETCH_JOB_SUBJECT


@pytest.mark.asyncio
async def test_submit_with_job_agent_mode() -> None:
    nc, manager, executor = await _setup()
    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)

    nc.request = AsyncMock(return_value=_make_reply(result.model_dump_json().encode()))

    task = await manager.create(job.app_id)
    executor.submit_with_job(task.task_id, job, mode=AnalysisMode.AGENT)
    await asyncio.sleep(0.1)

    calls = nc.request.call_args_list
    assert calls[0].args[0] == NATS_ANALYSIS_RUN_AGENT_SUBJECT
    assert calls[0].kwargs["timeout"] == 300.0


@pytest.mark.asyncio
async def test_submit_with_job_marks_failed_on_error() -> None:
    nc, manager, executor = await _setup()
    job = make_job()

    nc.request = AsyncMock(side_effect=TimeoutError("NATS timeout"))

    task = await manager.create(job.app_id)
    executor.submit_with_job(task.task_id, job)
    await asyncio.sleep(0.1)

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.FAILED
    assert updated.error is not None


@pytest.mark.asyncio
async def test_submit_with_job_marks_failed_on_analysis_error() -> None:
    nc, manager, executor = await _setup()
    job = make_job()

    nc.request = AsyncMock(return_value=_make_reply(orjson.dumps({"error": "Analysis failed"})))

    task = await manager.create(job.app_id)
    executor.submit_with_job(task.task_id, job)
    await asyncio.sleep(0.1)

    updated = await manager.get(task.task_id)
    assert updated is not None
    assert updated.status == TaskStatus.FAILED
    assert "Analysis failed" in (updated.error or "")
