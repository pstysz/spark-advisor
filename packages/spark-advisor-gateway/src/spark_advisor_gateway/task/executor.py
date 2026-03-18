from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import orjson
import structlog
from fastapi import HTTPException
from pydantic import TypeAdapter

from spark_advisor_models.model import AnalysisMode, AnalysisResult, ApplicationSummary, JobAnalysis
from spark_advisor_models.tracing import _build_trace_id_vars, get_tracer, inject_correlation_context

if TYPE_CHECKING:
    import nats.aio.client

    from spark_advisor_gateway.config import GatewaySettings
    from spark_advisor_gateway.task.manager import TaskManager

logger = structlog.stdlib.get_logger(__name__)

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
        headers = inject_correlation_context({})
        reply = await self._nc.request(
            self._settings.nats.apps_list_subject,
            orjson.dumps({"limit": limit}),
            timeout=self._settings.nats.list_apps_timeout,
            headers=headers,
        )
        data = orjson.loads(reply.data)
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=502, detail=data["error"])
        return _APP_LIST_ADAPTER.validate_python(data)

    def submit(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.AI) -> None:
        task = asyncio.create_task(self._execute(task_id, app_id, mode))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def submit_with_job(self, task_id: str, job: JobAnalysis, mode: AnalysisMode = AnalysisMode.AI) -> None:
        task = asyncio.create_task(self._execute_with_job(task_id, job, mode))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _execute(self, task_id: str, app_id: str, mode: AnalysisMode = AnalysisMode.AI) -> None:
        tracer = get_tracer()
        attrs = {"task_id": task_id, "app_id": app_id, "mode": mode.value}
        with tracer.start_as_current_span("gateway.execute_analysis", attributes=attrs):
            structlog.contextvars.clear_contextvars()
            structlog.contextvars.bind_contextvars(
                task_id=task_id, app_id=app_id, **_build_trace_id_vars(),
            )
            try:
                await self._tasks.mark_running(task_id)

                with tracer.start_as_current_span("gateway.nats_fetch_job", attributes={"app_id": app_id}):
                    headers = inject_correlation_context({})
                    fetch_job_reply = await self._nc.request(
                        self._settings.nats.job_fetch_subject,
                        orjson.dumps({"app_id": app_id}),
                        timeout=self._settings.nats.fetch_timeout,
                        headers=headers,
                    )

                job_payload = fetch_job_reply.data
                job_data = orjson.loads(job_payload)
                if "error" in job_data:
                    await self._tasks.mark_failed(task_id, f"Fetch failed: {job_data['error']}")
                    return

                await self._analyze_and_mark_completed(task_id, job_payload, mode)

            except Exception as e:
                logger.exception("Task %s failed", task_id)
                await self._tasks.mark_failed(task_id, str(e))

    async def _execute_with_job(self, task_id: str, job: JobAnalysis, mode: AnalysisMode = AnalysisMode.AI) -> None:
        tracer = get_tracer()
        attrs = {"task_id": task_id, "app_id": job.app_id, "mode": mode.value}
        with tracer.start_as_current_span("gateway.execute_analysis", attributes=attrs):
            structlog.contextvars.clear_contextvars()
            structlog.contextvars.bind_contextvars(
                task_id=task_id, app_id=job.app_id, **_build_trace_id_vars(),
            )
            try:
                await self._tasks.mark_running(task_id)
                job_data = orjson.dumps(job.model_dump(mode="json"))
                await self._analyze_and_mark_completed(task_id, job_data, mode)

            except Exception as e:
                logger.exception("Task %s failed", task_id)
                await self._tasks.mark_failed(task_id, str(e))

    async def _analyze_and_mark_completed(
        self, task_id: str, job_payload: bytes, mode: AnalysisMode,
    ) -> None:
        tracer = get_tracer()
        if mode == AnalysisMode.AGENT:
            subject = self._settings.nats.analysis_run_agent_subject
            timeout = self._settings.nats.analyze_agent_timeout
        else:
            subject = self._settings.nats.analysis_run_subject
            timeout = self._settings.nats.analyze_timeout

        with tracer.start_as_current_span("gateway.nats_analyze", attributes={"mode": mode.value}):
            headers = inject_correlation_context({})
            analyze_reply = await self._nc.request(
                subject,
                job_payload,
                timeout=timeout,
                headers=headers,
            )

        analyze_data = orjson.loads(analyze_reply.data)
        if "error" in analyze_data:
            await self._tasks.mark_failed(task_id, f"Analysis failed: {analyze_data['error']}")
            return

        result = AnalysisResult.model_validate(analyze_data)
        await self._tasks.mark_completed(task_id, result)
