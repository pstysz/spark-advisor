import logging

from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_analyzer.agent.orchestrator import AgentOrchestrator
from spark_advisor_analyzer.ai.client import AnthropicClient
from spark_advisor_analyzer.ai.service import LlmAnalysisService
from spark_advisor_analyzer.config import AnalyzerSettings
from spark_advisor_analyzer.handlers import router
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

settings = AnalyzerSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)


@app.on_startup
async def on_startup() -> None:
    logging.basicConfig(level=settings.log_level)
    logger = logging.getLogger(__name__)

    static = StaticAnalysisService(rules_for_threshold(settings.thresholds))

    llm_service: LlmAnalysisService | None = None
    agent_orch: AgentOrchestrator | None = None
    if settings.ai.enabled:
        try:
            client = AnthropicClient(settings.ai.api_timeout)
            client.__enter__()
            llm_service = LlmAnalysisService(client, settings.ai, settings.thresholds)
            agent_orch = AgentOrchestrator(client, static, settings.ai)
        except ValueError:
            logger.warning("ANTHROPIC_API_KEY not set — AI analysis disabled")

    orchestrator = AdviceOrchestrator(static, llm_service, agent_orch)
    app.context.set_global("orchestrator", orchestrator)

    logger.info(
        "Analyzer started: ai_enabled=%s agent_enabled=%s model=%s",
        settings.ai.enabled, agent_orch is not None, settings.ai.model,
    )


def main() -> None:
    import asyncio

    asyncio.run(app.run())
