from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from spark_advisor_gateway.api.health import create_health_router
from spark_advisor_gateway.api.routes import create_router
from spark_advisor_gateway.config import GatewaySettings, StateKey
from spark_advisor_gateway.task.executor import TaskExecutor
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.store import InMemoryTaskStore


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def task_manager() -> TaskManager:
    return TaskManager(InMemoryTaskStore())


@pytest.fixture
def mock_nc() -> AsyncMock:
    nc = AsyncMock()
    nc.is_connected = True
    return nc


@pytest.fixture
def task_executor(mock_nc: AsyncMock, task_manager: TaskManager, settings: GatewaySettings) -> TaskExecutor:
    return TaskExecutor(mock_nc, task_manager, settings)


@pytest.fixture
def app(
    mock_nc: AsyncMock, task_manager: TaskManager, task_executor: TaskExecutor, settings: GatewaySettings
) -> FastAPI:
    test_app = FastAPI()
    setattr(test_app.state, StateKey.NC, mock_nc)
    setattr(test_app.state, StateKey.SETTINGS, settings)
    setattr(test_app.state, StateKey.TASK_MANAGER, task_manager)
    setattr(test_app.state, StateKey.TASK_EXECUTOR, task_executor)
    test_app.include_router(create_health_router())
    test_app.include_router(create_router())
    return test_app


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
