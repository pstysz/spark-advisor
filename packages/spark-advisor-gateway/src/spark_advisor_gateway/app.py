from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import nats
import nats.aio.msg
import orjson
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from spark_advisor_gateway.api.health import create_health_router
from spark_advisor_gateway.api.routes import create_router
from spark_advisor_gateway.config import GatewaySettings, StateKey
from spark_advisor_gateway.task.executor import TaskExecutor
from spark_advisor_gateway.task.manager import TaskManager
from spark_advisor_gateway.task.store import TaskStore
from spark_advisor_gateway.ws.manager import ConnectionManager
from spark_advisor_gateway.ws.routes import router as ws_router
from spark_advisor_models.model import DataSource, JobAnalysis

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logger = logging.getLogger(__name__)


async def handle_polling_message(
    msg: nats.aio.msg.Msg,
    task_manager: TaskManager,
    task_executor: TaskExecutor,
    settings: GatewaySettings,
) -> None:
    try:
        job = JobAnalysis.model_validate(orjson.loads(msg.data))
        result = await task_manager.create_if_not_active(
            job.app_id, rerun=False, data_source=DataSource.HS_POLLER,
        )
        if result is None:
            return
        task, created = result
        if not created:
            return
        task_executor.submit_with_job(task.task_id, job, settings.nats.polling_analysis_mode)
        logger.info("Created task %s for polled app %s", task.task_id, job.app_id)
    except Exception:
        logger.exception("Failed to handle polling message")


def create_app(settings: GatewaySettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nc = await nats.connect(settings.nats.url)
        logger.info("Connected to NATS: %s", settings.nats.url)

        store = TaskStore(settings.database_url)
        await store.init()

        ws_manager = ConnectionManager(settings.ws_heartbeat_interval)
        task_manager = TaskManager(store, on_status_change=ws_manager.broadcast)
        task_executor = TaskExecutor(nc, task_manager, settings)

        async def _polling_cb(msg: nats.aio.msg.Msg) -> None:
            await handle_polling_message(msg, task_manager, task_executor, settings)

        sub = await nc.subscribe(
            settings.nats.analysis_submit_subject,
            cb=_polling_cb,
        )
        logger.info("Subscribed to %s", settings.nats.analysis_submit_subject)

        await ws_manager.start()

        setattr(_app.state, StateKey.NC, nc)
        setattr(_app.state, StateKey.SETTINGS, settings)
        setattr(_app.state, StateKey.TASK_MANAGER, task_manager)
        setattr(_app.state, StateKey.TASK_EXECUTOR, task_executor)
        setattr(_app.state, StateKey.CONNECTION_MANAGER, ws_manager)

        yield

        await ws_manager.stop()
        await sub.unsubscribe()
        await nc.drain()
        await store.close()
        logger.info("Disconnected from NATS")

    app = FastAPI(title="Spark Advisor Gateway", lifespan=lifespan)

    app.include_router(create_health_router())
    app.include_router(create_router())
    app.include_router(ws_router)

    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="static-assets")

        @app.get("/{path:path}", include_in_schema=False)
        async def spa_fallback(path: str) -> FileResponse:
            return FileResponse(static_dir / "index.html")

    return app


def main() -> None:
    settings = GatewaySettings()
    logging.basicConfig(level=settings.log_level)
    app = create_app(settings)
    uvicorn.run(app, host=settings.server.host, port=settings.server.port)
