import asyncio

import structlog
from faststream.context import Context
from faststream.nats import NatsMessage, NatsRouter
from pydantic import BaseModel

from spark_advisor_analyzer.config import ContextKey
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.defaults import (
    NATS_ANALYSIS_RESULT_SUBJECT,
    NATS_ANALYSIS_RUN_AGENT_SUBJECT,
    NATS_ANALYSIS_RUN_SUBJECT,
)
from spark_advisor_models.logging import bind_nats_context
from spark_advisor_models.model import AnalysisMode, AnalysisResult, JobAnalysis
from spark_advisor_models.tracing import get_tracer

router = NatsRouter()
logger = structlog.stdlib.get_logger(__name__)


class ErrorResponse(BaseModel):
    error: str


@router.subscriber(NATS_ANALYSIS_RUN_SUBJECT)
@router.publisher(NATS_ANALYSIS_RESULT_SUBJECT)
async def handle_analyze(
    job: JobAnalysis,
    msg: NatsMessage,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> AnalysisResult | ErrorResponse:
    bind_nats_context(msg.headers, app_id=job.app_id, service=service_name)
    attrs = {"app_id": job.app_id, "mode": AnalysisMode.AI.value}
    with get_tracer().start_as_current_span("analyzer.analyze", attributes=attrs):
        try:
            return await asyncio.to_thread(orchestrator.run, job)
        except Exception as e:
            logger.exception("Analysis failed for %s", job.app_id)
            return ErrorResponse(error=str(e))


@router.subscriber(NATS_ANALYSIS_RUN_AGENT_SUBJECT)
@router.publisher(NATS_ANALYSIS_RESULT_SUBJECT)
async def handle_agent_analyze(
    job: JobAnalysis,
    msg: NatsMessage,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> AnalysisResult | ErrorResponse:
    bind_nats_context(msg.headers, app_id=job.app_id, service=service_name)
    attrs = {"app_id": job.app_id, "mode": AnalysisMode.AGENT.value}
    with get_tracer().start_as_current_span("analyzer.analyze", attributes=attrs):
        try:
            return await asyncio.to_thread(orchestrator.run, job, mode=AnalysisMode.AGENT)
        except Exception as e:
            logger.exception("Agent analysis failed for %s", job.app_id)
            return ErrorResponse(error=str(e))
