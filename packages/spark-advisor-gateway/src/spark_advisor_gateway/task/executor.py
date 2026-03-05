from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import orjson

from spark_advisor_models.model import AnalysisResult
from spark_advisor_models.model.output import AnalysisMode

if TYPE_CHECKING:
    import nats.aio.client

    from spark_advisor_gateway.config import GatewaySettings
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)


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
        self._background_tasks: set[asyncio.Task[None]] = set()

    def submit(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.STANDARD) -> None:
        task = asyncio.create_task(self._execute(task_id, app_id, mode))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _execute(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.STANDARD) -> None:
        try:
            await self._tasks.mark_running(task_id)

            fetch_reply = await self._nc.request(
                self._settings.nats.fetch_subject,
                orjson.dumps({"app_id": app_id}),
                timeout=self._settings.nats.fetch_timeout,
            )

            fetch_data = orjson.loads(fetch_reply.data)
            if "error" in fetch_data:
                await self._tasks.mark_failed(task_id, f"Fetch failed: {fetch_data['error']}")
                return

            if mode == AnalysisMode.AGENT:
                subject = self._settings.nats.analyze_agent_subject
                timeout = self._settings.nats.analyze_agent_timeout
            else:
                subject = self._settings.nats.analyze_subject
                timeout = self._settings.nats.analyze_timeout

            analyze_reply = await self._nc.request(
                subject,
                fetch_reply.data,
                timeout=timeout,
            )

            analyze_data = orjson.loads(analyze_reply.data)
            if "error" in analyze_data:
                await self._tasks.mark_failed(task_id, f"Analysis failed: {analyze_data['error']}")
                return

            result = AnalysisResult.model_validate(analyze_data)
            await self._tasks.mark_completed(task_id, result)

        except Exception as e:
            logger.exception("Task %s failed", task_id)
            await self._tasks.mark_failed(task_id, str(e))
