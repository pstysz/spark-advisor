from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import orjson

from spark_advisor_models.model import AnalysisResult

if TYPE_CHECKING:
    import nats.aio.client

    from spark_advisor_gateway.config import GatewaySettings
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)

_background_tasks: set[asyncio.Task[None]] = set()


class TaskExecutor:
    def __init__(
        self,
        nc: nats.aio.client.Client,
        task_manager: TaskManager,
        settings: GatewaySettings,
    ) -> None:
        self._nc = nc
        self._tasks = task_manager
        self._settings = settings

    def submit(self, task_id: str, app_id: str) -> None:
        task = asyncio.create_task(self._execute(task_id, app_id))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)

    async def _execute(self, task_id: str, app_id: str) -> None:
        try:
            self._tasks.mark_running(task_id)

            fetch_reply = await self._nc.request(
                self._settings.nats.fetch_subject,
                orjson.dumps({"app_id": app_id}),
                timeout=self._settings.nats.fetch_timeout,
            )

            analyze_reply = await self._nc.request(
                self._settings.nats.analyze_subject,
                fetch_reply.data,
                timeout=self._settings.nats.analyze_timeout,
            )

            result = AnalysisResult.model_validate_json(analyze_reply.data)
            self._tasks.mark_completed(task_id, result)

        except Exception as e:
            logger.exception("Task %s failed", task_id)
            self._tasks.mark_failed(task_id, str(e))
