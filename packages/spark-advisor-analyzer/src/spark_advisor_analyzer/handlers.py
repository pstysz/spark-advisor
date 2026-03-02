import asyncio
import logging
from typing import Any

from faststream.context import Context
from faststream.nats import NatsRouter

from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AnalysisResult, JobAnalysis

router = NatsRouter()
logger = logging.getLogger(__name__)


@router.subscriber("analyze.request")
@router.publisher("analyze.result")
async def handle_analyze(
    job: JobAnalysis,
    orchestrator: AdviceOrchestrator = Context("orchestrator"),  # type: ignore[assignment]  # noqa: B008
) -> AnalysisResult | dict[str, Any]:
    try:
        return await asyncio.to_thread(orchestrator.run, job)
    except Exception as e:
        logger.exception("Analysis failed for %s", job.app_id)
        return {"error": str(e)}
