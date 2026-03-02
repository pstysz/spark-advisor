import asyncio

from faststream.context import Context
from faststream.nats import NatsRouter

from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AnalysisResult, JobAnalysis

router = NatsRouter()


@router.subscriber("analyze.request")
@router.publisher("analyze.result")
async def handle_analyze(
    job: JobAnalysis,
    orchestrator: AdviceOrchestrator = Context("orchestrator"),  # type: ignore[assignment]  # noqa: B008
) -> AnalysisResult:
    return await asyncio.to_thread(orchestrator.run, job)
