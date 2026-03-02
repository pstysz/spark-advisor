from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import nats
import uvicorn
from fastapi import FastAPI

from spark_advisor_gateway.api.health import create_health_router
from spark_advisor_gateway.api.routes import create_router
from spark_advisor_gateway.config import GatewaySettings
from spark_advisor_gateway.task.executor import TaskExecutor
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.store import InMemoryTaskStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: GatewaySettings = app.state.settings

    nc = await nats.connect(settings.nats.url)
    logger.info("Connected to NATS: %s", settings.nats.url)

    store = InMemoryTaskStore(max_size=settings.max_stored_tasks)
    task_manager = TaskManager(store)
    task_executor = TaskExecutor(nc, task_manager, settings)

    app.state.nc = nc
    app.state.task_manager = task_manager
    app.state.task_executor = task_executor

    yield

    await nc.drain()
    logger.info("Disconnected from NATS")


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    if settings is None:
        settings = GatewaySettings()

    app = FastAPI(title="Spark Advisor Gateway", lifespan=lifespan)
    app.state.settings = settings

    app.include_router(create_health_router())
    app.include_router(create_router())

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = GatewaySettings()
    app = create_app(settings)
    uvicorn.run(app, host=settings.server.host, port=settings.server.port)
