from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

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
