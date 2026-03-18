from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from spark_advisor_gateway.metrics import task_duration_observe, tasks_total_inc

if TYPE_CHECKING:
    from httpx import AsyncClient


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_format(client: AsyncClient) -> None:
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"] or "text/plain" in response.headers.get("content-type", "")
    body = response.text
    assert "http_request" in body or "HELP" in body


@pytest.mark.asyncio
async def test_metrics_contains_custom_counters(client: AsyncClient) -> None:
    response = await client.get("/metrics")
    body = response.text
    assert "sa_tasks_total" in body
    assert "sa_task_duration_seconds" in body


@pytest.mark.asyncio
async def test_tasks_total_inc_increments_counter(client: AsyncClient) -> None:
    tasks_total_inc("completed")
    tasks_total_inc("completed")
    tasks_total_inc("failed")
    response = await client.get("/metrics")
    body = response.text
    assert 'sa_tasks_total{status="completed"}' in body
    assert 'sa_tasks_total{status="failed"}' in body


@pytest.mark.asyncio
async def test_task_duration_observe_records_histogram(client: AsyncClient) -> None:
    task_duration_observe("ai", 5.5)
    task_duration_observe("ai", 12.0)
    response = await client.get("/metrics")
    body = response.text
    assert "sa_task_duration_seconds_count" in body
    assert "sa_task_duration_seconds_sum" in body
