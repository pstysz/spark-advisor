import asyncio
import logging

from faststream.context import Context
from faststream.nats import NatsRouter
from pydantic import BaseModel

from spark_advisor_analyzer.config import ContextKey
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AnalysisMode, AnalysisResult, JobAnalysis

router = NatsRouter()
logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    error: str


@router.subscriber("analyze.request")
@router.publisher("analyze.result")
async def handle_analyze(
    job: JobAnalysis,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
) -> AnalysisResult | ErrorResponse:
    try:
        return await asyncio.to_thread(orchestrator.run, job)
    except Exception as e:
        logger.exception("Analysis failed for %s", job.app_id)
        return ErrorResponse(error=str(e))


@router.subscriber("analyze.agent.request")
@router.publisher("analyze.result")
async def handle_agent_analyze(
    job: JobAnalysis,
    orchestrator: AdviceOrchestrator = Context(ContextKey.ORCHESTRATOR),  # type: ignore[assignment]  # noqa: B008
) -> AnalysisResult | ErrorResponse:
    try:
        return await asyncio.to_thread(orchestrator.run, job, mode=AnalysisMode.AGENT)
    except Exception as e:
        logger.exception("Agent analysis failed for %s", job.app_id)
        return ErrorResponse(error=str(e))
