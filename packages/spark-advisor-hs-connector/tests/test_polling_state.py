import pytest

from spark_advisor_hs_connector.store import PollingStore

_DB_URL = "sqlite+aiosqlite:///:memory:"


async def _make_state(max_size: int = 10_000) -> PollingStore:
    state = PollingStore(_DB_URL, max_size=max_size)
    await state.init()
    return state


class TestPollingState:
    @pytest.mark.asyncio
    async def test_new_app_not_processed(self) -> None:
        state = await _make_state()
        assert not await state.is_processed("app-001")

    @pytest.mark.asyncio
    async def test_mark_processed(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        assert await state.is_processed("app-001")

    @pytest.mark.asyncio
    async def test_filter_new(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        await state.mark_processed("app-003")
        result = await state.filter_new(["app-001", "app-002", "app-003", "app-004"])
        assert result == ["app-002", "app-004"]

    @pytest.mark.asyncio
    async def test_processed_count(self) -> None:
        state = await _make_state()
        assert await state.processed_count() == 0
        await state.mark_processed("app-001")
        await state.mark_processed("app-002")
        assert await state.processed_count() == 2

    @pytest.mark.asyncio
    async def test_idempotent_mark(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        await state.mark_processed("app-001")
        assert await state.processed_count() == 1

    @pytest.mark.asyncio
    async def test_eviction_removes_oldest(self) -> None:
        state = await _make_state(max_size=3)
        await state.mark_processed("app-001")
        await state.mark_processed("app-002")
        await state.mark_processed("app-003")
        await state.mark_processed("app-004")
        assert await state.processed_count() == 3
        assert not await state.is_processed("app-001")
        assert await state.is_processed("app-002")
        assert await state.is_processed("app-003")
        assert await state.is_processed("app-004")

    @pytest.mark.asyncio
    async def test_eviction_fifo_order(self) -> None:
        state = await _make_state(max_size=2)
        await state.mark_processed("a")
        await state.mark_processed("b")
        await state.mark_processed("c")
        await state.mark_processed("d")
        assert not await state.is_processed("a")
        assert not await state.is_processed("b")
        assert await state.is_processed("c")
        assert await state.is_processed("d")

    @pytest.mark.asyncio
    async def test_filter_new_and_mark_returns_new_only(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        new = await state.filter_new_and_mark(["app-001", "app-002", "app-003"])
        assert new == ["app-002", "app-003"]

    @pytest.mark.asyncio
    async def test_filter_new_and_mark_marks_atomically(self) -> None:
        state = await _make_state()
        await state.filter_new_and_mark(["app-001", "app-002"])
        assert await state.is_processed("app-001")
        assert await state.is_processed("app-002")
        assert await state.processed_count() == 2

    @pytest.mark.asyncio
    async def test_filter_new_and_mark_empty_input(self) -> None:
        state = await _make_state()
        assert await state.filter_new_and_mark([]) == []

    @pytest.mark.asyncio
    async def test_filter_new_and_mark_all_known(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        assert await state.filter_new_and_mark(["app-001"]) == []

    @pytest.mark.asyncio
    async def test_filter_new_and_mark_respects_eviction(self) -> None:
        state = await _make_state(max_size=2)
        await state.mark_processed("app-001")
        await state.filter_new_and_mark(["app-002", "app-003"])
        assert not await state.is_processed("app-001")
        assert await state.is_processed("app-002")
        assert await state.is_processed("app-003")

    @pytest.mark.asyncio
    async def test_remove_existing(self) -> None:
        state = await _make_state()
        await state.mark_processed("app-001")
        await state.remove("app-001")
        assert not await state.is_processed("app-001")
        assert await state.processed_count() == 0

    @pytest.mark.asyncio
    async def test_remove_nonexistent_is_noop(self) -> None:
        state = await _make_state()
        await state.remove("nonexistent")
        assert await state.processed_count() == 0
