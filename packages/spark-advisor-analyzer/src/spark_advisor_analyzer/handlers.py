import asyncio

import structlog
from faststream.context import Context
from faststream.nats import NatsMessage, NatsRouter

from spark_advisor_analyzer.config import ContextKey
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.defaults import (
    NATS_ANALYSIS_RESULT_SUBJECT,
    NATS_ANALYSIS_RUN_AGENT_SUBJECT,
    NATS_ANALYSIS_RUN_SUBJECT,
)
from spark_advisor_models.logging import nats_handler_context
from spark_advisor_models.model import AnalysisMode, AnalysisResult, ErrorResponse, JobAnalysis

router = NatsRouter()
logger = structlog.stdlib.get_logger(__name__)


async def _run_analysis(
    job: JobAnalysis,
    msg: NatsMessage,
    orchestrator: AdviceOrchestrator,
    service_name: str,
    mode: AnalysisMode,
) -> AnalysisResult | ErrorResponse:
    async with nats_handler_context(
        msg.headers, "analyzer.analyze",
        {"app_id": job.app_id, "mode": mode.value},
        app_id=job.app_id, service=service_name,
    ):
        try:
            return await asyncio.to_thread(orchestrator.run, job, mode=mode)
        except Exception as e:
            logger.exception("Analysis failed for %s", job.app_id)
            return ErrorResponse(error=str(e))


@router.subscriber(NATS_ANALYSIS_RUN_SUBJECT)
@router.publisher(NATS_ANALYSIS_RESULT_SUBJECT)
async def handle_analyze(
    job: JobAnalysis,
    msg: NatsMessage,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> AnalysisResult | ErrorResponse:
    return await _run_analysis(job, msg, orchestrator, service_name, AnalysisMode.AI)


@router.subscriber(NATS_ANALYSIS_RUN_AGENT_SUBJECT)
@router.publisher(NATS_ANALYSIS_RESULT_SUBJECT)
async def handle_agent_analyze(
    job: JobAnalysis,
    msg: NatsMessage,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> AnalysisResult | ErrorResponse:
    return await _run_analysis(job, msg, orchestrator, service_name, AnalysisMode.AGENT)
