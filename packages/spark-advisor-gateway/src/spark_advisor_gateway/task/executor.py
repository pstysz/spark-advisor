from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

import orjson
from fastapi import HTTPException
from pydantic import TypeAdapter

from spark_advisor_models.model import AnalysisMode, AnalysisResult, ApplicationSummary

if TYPE_CHECKING:
    import nats.aio.client

    from spark_advisor_gateway.config import GatewaySettings
    from spark_advisor_gateway.task.manager import TaskManager

logger = logging.getLogger(__name__)

_APP_LIST_ADAPTER: TypeAdapter[list[ApplicationSummary]] = TypeAdapter(list[ApplicationSummary])


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

    async def list_applications(self, limit: int) -> list[ApplicationSummary]:
        reply = await self._nc.request(
            self._settings.nats.list_applications_subject,
            orjson.dumps({"limit": limit}),
            timeout=self._settings.nats.list_apps_timeout,
        )
        data = orjson.loads(reply.data)
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=502, detail=data["error"])
        return _APP_LIST_ADAPTER.validate_python(data)

    def submit(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.AI) -> None:
        task = asyncio.create_task(self._execute(task_id, app_id, mode))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _execute(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.AI) -> None:
        try:
            await self._tasks.mark_running(task_id)

            fetch_job_reply = await self._nc.request(
                self._settings.nats.fetch_job_subject,
                orjson.dumps({"app_id": app_id}),
                timeout=self._settings.nats.fetch_timeout,
            )

            job_data = orjson.loads(fetch_job_reply.data)
            if "error" in job_data:
                await self._tasks.mark_failed(task_id, f"Fetch failed: {job_data['error']}")
                return

            if mode == AnalysisMode.AGENT:
                subject = self._settings.nats.analyze_agent_request_subject
                timeout = self._settings.nats.analyze_agent_timeout
            else:
                subject = self._settings.nats.analyze_request_subject
                timeout = self._settings.nats.analyze_timeout

            analyze_reply = await self._nc.request(
                subject,
                job_data,
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
