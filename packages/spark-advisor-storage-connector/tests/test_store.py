"""Tests for polling store."""

from __future__ import annotations

from collections.abc import AsyncGenerator  # noqa: TC003

import pytest
import pytest_asyncio

from spark_advisor_storage_connector.store import PollingStore


@pytest_asyncio.fixture
async def polling_store() -> AsyncGenerator[PollingStore, None]:
    """In-memory polling store for testing."""
    store = PollingStore(database_url="sqlite+aiosqlite:///:memory:", max_size=10)
    await store.init()
    try:
        yield store
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_filter_new_and_mark(polling_store: PollingStore) -> None:
    """Test filtering new paths and marking them as processed."""
    paths = [
        "/spark-events/app-123.json",
        "/spark-events/app-456.json",
        "/spark-events/app-789.json"
    ]

    # First call - all paths are new
    new_paths = await polling_store.filter_new_and_mark(paths)
    assert set(new_paths) == set(paths)

    # Second call - no paths are new
    new_paths = await polling_store.filter_new_and_mark(paths)
    assert new_paths == []

    # Third call with mixed paths
    mixed_paths = [*paths, "/spark-events/app-999.json"]
    new_paths = await polling_store.filter_new_and_mark(mixed_paths)
    assert new_paths == ["/spark-events/app-999.json"]


@pytest.mark.asyncio
async def test_filter_new_and_mark_empty_list(polling_store: PollingStore) -> None:
    """Test filtering with empty list."""
    new_paths = await polling_store.filter_new_and_mark([])
    assert new_paths == []


@pytest.mark.asyncio
async def test_remove(polling_store: PollingStore) -> None:
    """Test removing a processed path."""
    paths = ["/spark-events/app-123.json", "/spark-events/app-456.json"]

    # Mark paths as processed
    await polling_store.filter_new_and_mark(paths)

    # Remove one path
    await polling_store.remove("/spark-events/app-123.json")

    # Check that removed path is new again
    new_paths = await polling_store.filter_new_and_mark(paths)
    assert new_paths == ["/spark-events/app-123.json"]


@pytest.mark.asyncio
async def test_eviction() -> None:
    """Test eviction when max_size is exceeded."""
    # Create store with max_size=2
    store = PollingStore(database_url="sqlite+aiosqlite:///:memory:", max_size=2)
    await store.init()

    try:
        # Add 3 paths (exceeds max_size)
        paths1 = ["/spark-events/app-1.json"]
        paths2 = ["/spark-events/app-2.json"]
        paths3 = ["/spark-events/app-3.json"]

        await store.filter_new_and_mark(paths1)
        await store.filter_new_and_mark(paths2)
        await store.filter_new_and_mark(paths3)  # This should evict app-1.json

        # Check that oldest (app-1.json) was evicted
        new_paths = await store.filter_new_and_mark(paths1 + paths2 + paths3)
        assert new_paths == ["/spark-events/app-1.json"]

    finally:
        await store.close()


@pytest.mark.asyncio
async def test_multiple_filter_calls(polling_store: PollingStore) -> None:
    """Test multiple filter calls with overlapping paths."""
    # First batch
    batch1 = ["/spark-events/app-1.json", "/spark-events/app-2.json"]
    new1 = await polling_store.filter_new_and_mark(batch1)
    assert set(new1) == set(batch1)

    # Second batch with one overlapping path
    batch2 = ["/spark-events/app-2.json", "/spark-events/app-3.json"]
    new2 = await polling_store.filter_new_and_mark(batch2)
    assert new2 == ["/spark-events/app-3.json"]

    # Third batch with all known paths
    new3 = await polling_store.filter_new_and_mark(batch1 + batch2)
    assert new3 == []


@pytest.mark.asyncio
async def test_init_creates_tables(polling_store: PollingStore) -> None:
    """Test that init creates the necessary tables."""
    # This is implicitly tested by the fixture working
    # We can add a path to verify the table exists
    paths = ["/spark-events/test.json"]
    new_paths = await polling_store.filter_new_and_mark(paths)
    assert new_paths == paths
