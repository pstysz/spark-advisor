import pytest
from faststream.nats import TestNatsBroker

from spark_advisor_analyzer.app import app, broker
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AnalysisResult
from spark_advisor_models.testing import make_job
from spark_advisor_rules import StaticAnalysisService


@pytest.mark.asyncio
async def test_analyze_request_returns_result() -> None:
    orchestrator = AdviceOrchestrator(StaticAnalysisService())
    job = make_job()

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("orchestrator", orchestrator)
        result = await br.request(
            job.model_dump(mode="json"),
            subject="analyze.request",
            timeout=10.0,
        )
        parsed = AnalysisResult.model_validate_json(result.body)
        assert parsed.app_id == job.app_id
        assert parsed.ai_report is None
