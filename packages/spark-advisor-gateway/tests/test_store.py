from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from spark_advisor_gateway.task.models import AnalysisTask, TaskStatus
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_models.model import AnalysisMode, AnalysisResult, DataSource
from spark_advisor_models.testing import make_job


async def _make_store() -> TaskStore:
    store = TaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()
    return store


def _make_task(
    task_id: str = "tid-1",
    app_id: str = "app-1",
    mode: AnalysisMode = AnalysisMode.AI,
    data_source: DataSource = DataSource.HS_MANUAL,
    status: TaskStatus = TaskStatus.PENDING,
    created_at: datetime | None = None,
) -> AnalysisTask:
    return AnalysisTask(
        task_id=task_id,
        app_id=app_id,
        mode=mode,
        data_source=data_source,
        status=status,
        created_at=created_at or datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_create_and_get_roundtrip() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.task_id == task.task_id
    assert loaded.app_id == task.app_id
    assert loaded.mode == AnalysisMode.AI
    assert loaded.status == TaskStatus.PENDING
    assert loaded.created_at.tzinfo == UTC


@pytest.mark.asyncio
async def test_create_and_get_roundtrip_agent_mode() -> None:
    store = await _make_store()
    task = _make_task(mode=AnalysisMode.AGENT)
    await store.create(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.mode == AnalysisMode.AGENT


@pytest.mark.asyncio
async def test_get_returns_none_for_unknown() -> None:
    store = await _make_store()
    assert await store.get("nonexistent") is None


@pytest.mark.asyncio
async def test_update_status_transition() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)

    task.status = TaskStatus.RUNNING
    task.started_at = datetime.now(UTC)
    await store.update(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.status == TaskStatus.RUNNING
    assert loaded.started_at is not None
    assert loaded.started_at.tzinfo == UTC


@pytest.mark.asyncio
async def test_update_nonexistent_is_noop() -> None:
    store = await _make_store()
    task = _make_task(task_id="nonexistent")
    await store.update(task)
    assert await store.get("nonexistent") is None


@pytest.mark.asyncio
async def test_list_recent_ordering() -> None:
    store = await _make_store()
    now = datetime.now(UTC)
    await store.create(_make_task(task_id="t1", created_at=now - timedelta(seconds=2)))
    await store.create(_make_task(task_id="t2", created_at=now - timedelta(seconds=1)))
    await store.create(_make_task(task_id="t3", created_at=now))

    tasks = await store.list_recent(10)
    assert [t.task_id for t in tasks] == ["t3", "t2", "t1"]


@pytest.mark.asyncio
async def test_list_recent_respects_limit() -> None:
    store = await _make_store()
    for i in range(5):
        await store.create(_make_task(task_id=f"t-{i}"))
    assert len(await store.list_recent(2)) == 2


@pytest.mark.asyncio
async def test_result_json_roundtrip() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)

    job = make_job()
    result = AnalysisResult(app_id=job.app_id, job=job, rule_results=[], ai_report=None)
    task.status = TaskStatus.COMPLETED
    task.completed_at = datetime.now(UTC)
    task.result = result
    await store.update(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.result is not None
    assert loaded.result.app_id == job.app_id
    assert loaded.result.job.app_id == job.app_id


@pytest.mark.asyncio
async def test_error_with_unicode() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)
    task.status = TaskStatus.FAILED
    task.error = "Błąd połączenia: タイムアウト 🔥"
    await store.update(task)
    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.error == "Błąd połączenia: タイムアウト 🔥"


@pytest.mark.asyncio
async def test_error_with_long_string() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)
    long_error = "x" * 10_000
    task.status = TaskStatus.FAILED
    task.error = long_error
    await store.update(task)
    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.error == long_error


@pytest.mark.asyncio
async def test_list_filtered_by_status() -> None:
    store = await _make_store()
    await store.create(_make_task(task_id="t1", status=TaskStatus.PENDING))
    await store.create(_make_task(task_id="t2", status=TaskStatus.RUNNING))
    await store.create(_make_task(task_id="t3", status=TaskStatus.PENDING))

    tasks, total = await store.list_filtered(status=TaskStatus.PENDING)
    assert total == 2
    assert all(t.status == TaskStatus.PENDING for t in tasks)


@pytest.mark.asyncio
async def test_list_filtered_by_app_id() -> None:
    store = await _make_store()
    await store.create(_make_task(task_id="t1", app_id="app-A"))
    await store.create(_make_task(task_id="t2", app_id="app-B"))
    await store.create(_make_task(task_id="t3", app_id="app-A"))

    tasks, total = await store.list_filtered(app_id="app-A")
    assert total == 2
    assert all(t.app_id == "app-A" for t in tasks)


@pytest.mark.asyncio
async def test_list_filtered_with_offset_and_limit() -> None:
    store = await _make_store()
    now = datetime.now(UTC)
    for i in range(5):
        await store.create(_make_task(task_id=f"t-{i}", created_at=now + timedelta(seconds=i)))

    tasks, total = await store.list_filtered(limit=2, offset=1)
    assert total == 5
    assert len(tasks) == 2
    assert tasks[0].task_id == "t-3"
    assert tasks[1].task_id == "t-2"


@pytest.mark.asyncio
async def test_list_filtered_returns_total_count() -> None:
    store = await _make_store()
    for i in range(10):
        await store.create(_make_task(task_id=f"t-{i}"))

    _, total = await store.list_filtered(limit=3)
    assert total == 10


@pytest.mark.asyncio
async def test_count_by_app_ids() -> None:
    store = await _make_store()
    await store.create(_make_task(task_id="t1", app_id="app-A"))
    await store.create(_make_task(task_id="t2", app_id="app-A"))
    await store.create(_make_task(task_id="t3", app_id="app-B"))
    await store.create(_make_task(task_id="t4", app_id="app-C"))

    counts = await store.count_by_app_ids(["app-A", "app-B", "app-D"])
    assert counts["app-A"] == 2
    assert counts["app-B"] == 1
    assert "app-D" not in counts


@pytest.mark.asyncio
async def test_count_by_app_ids_empty() -> None:
    store = await _make_store()
    assert await store.count_by_app_ids([]) == {}


@pytest.mark.asyncio
async def test_count_by_status() -> None:
    store = await _make_store()
    await store.create(_make_task(task_id="t1", status=TaskStatus.PENDING))
    await store.create(_make_task(task_id="t2", status=TaskStatus.RUNNING))
    await store.create(_make_task(task_id="t3", status=TaskStatus.PENDING))
    await store.create(_make_task(task_id="t4", status=TaskStatus.COMPLETED))

    counts = await store.count_by_status()
    assert counts[TaskStatus.PENDING] == 2
    assert counts[TaskStatus.RUNNING] == 1
    assert counts[TaskStatus.COMPLETED] == 1


@pytest.mark.asyncio
async def test_count_by_status_empty() -> None:
    store = await _make_store()
    counts = await store.count_by_status()
    assert counts == {}


@pytest.mark.asyncio
async def test_concurrent_operations() -> None:
    store = await _make_store()

    async def create_task(idx: int) -> None:
        await store.create(_make_task(task_id=f"concurrent-{idx}", app_id=f"app-{idx}"))

    await asyncio.gather(*[create_task(i) for i in range(10)])

    tasks = await store.list_recent(20)
    assert len(tasks) == 10


@pytest.mark.asyncio
async def test_init_creates_table() -> None:
    store = TaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()

    task = _make_task()
    await store.create(task)
    loaded = await store.get(task.task_id)
    assert loaded is not None


@pytest.mark.asyncio
async def test_init_idempotent() -> None:
    store = TaskStore("sqlite+aiosqlite:///:memory:")
    await store.init()
    await store.init()

    task = _make_task()
    await store.create(task)
    loaded = await store.get(task.task_id)
    assert loaded is not None


@pytest.mark.asyncio
async def test_data_source_roundtrip() -> None:
    store = await _make_store()
    task = _make_task(data_source=DataSource.HS_POLLER)
    await store.create(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.data_source == DataSource.HS_POLLER


@pytest.mark.asyncio
async def test_default_data_source_is_hs_manual() -> None:
    store = await _make_store()
    task = _make_task()
    await store.create(task)

    loaded = await store.get(task.task_id)
    assert loaded is not None
    assert loaded.data_source == DataSource.HS_MANUAL


@pytest.mark.asyncio
async def test_count_by_data_source_since() -> None:
    store = await _make_store()
    since = datetime.now(UTC) - timedelta(hours=1)
    await store.create(_make_task(task_id="t1", data_source=DataSource.HS_MANUAL))
    await store.create(_make_task(task_id="t2", data_source=DataSource.HS_MANUAL))
    await store.create(_make_task(task_id="t3", data_source=DataSource.HS_POLLER))

    rows = await store.count_by_data_source_since(since)
    counts = dict(rows)
    assert counts["hs_manual"] == 2
    assert counts["hs_poller"] == 1


@pytest.mark.asyncio
async def test_list_filtered_by_data_source() -> None:
    store = await _make_store()
    await store.create(_make_task(task_id="t1", data_source=DataSource.HS_MANUAL))
    await store.create(_make_task(task_id="t2", data_source=DataSource.HS_POLLER))
    await store.create(_make_task(task_id="t3", data_source=DataSource.HS_MANUAL))

    tasks, total = await store.list_filtered(data_source=DataSource.HS_MANUAL)
    assert total == 2
    assert all(t.data_source == DataSource.HS_MANUAL for t in tasks)
