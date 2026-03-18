import orjson
import pytest
from faststream.nats import TestNatsBroker

from spark_advisor_analyzer.app import app, broker
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.defaults import NATS_ANALYSIS_RUN_AGENT_SUBJECT, NATS_ANALYSIS_RUN_SUBJECT
from spark_advisor_models.testing import make_job
from spark_advisor_rules import StaticAnalysisService


@pytest.mark.asyncio
async def test_ai_analyze_without_llm_returns_error() -> None:
    orchestrator = AdviceOrchestrator(StaticAnalysisService())
    job = make_job()

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("orchestrator", orchestrator)
        app.context.set_global("service_name", "spark-advisor-analyzer")
        result = await br.request(
            job.model_dump(mode="json"),
            subject=NATS_ANALYSIS_RUN_SUBJECT,
            timeout=10.0,
        )
        parsed = orjson.loads(result.body)
        assert "error" in parsed
        assert "AI mode" in parsed["error"]


@pytest.mark.asyncio
async def test_agent_analyze_without_agent_returns_error() -> None:
    orchestrator = AdviceOrchestrator(StaticAnalysisService())
    job = make_job()

    async with TestNatsBroker(broker, with_real=False) as br:
        app.context.set_global("orchestrator", orchestrator)
        app.context.set_global("service_name", "spark-advisor-analyzer")
        result = await br.request(
            job.model_dump(mode="json"),
            subject=NATS_ANALYSIS_RUN_AGENT_SUBJECT,
            timeout=10.0,
        )
        parsed = orjson.loads(result.body)
        assert "error" in parsed
        assert "Agent mode" in parsed["error"]
