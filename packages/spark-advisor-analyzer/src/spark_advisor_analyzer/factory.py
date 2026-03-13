from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

from spark_advisor_analyzer.agent.orchestrator import AgentOrchestrator
from spark_advisor_analyzer.ai.service import LlmAnalysisService
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.defaults import DEFAULT_MODEL, DEFAULT_THRESHOLDS
from spark_advisor_models.model import AnalysisMode
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

if TYPE_CHECKING:
    from collections.abc import Iterator

    from spark_advisor_analyzer.ai.client import AnthropicClient
    from spark_advisor_models.config import AiSettings, Thresholds


def create_analysis_stack(
    *,
    client: AnthropicClient | None,
    ai_settings: AiSettings | None = None,
    thresholds: Thresholds,
) -> AdviceOrchestrator:
    static = StaticAnalysisService(rules_for_threshold(thresholds))

    # Both services are always created when a client is provided because the
    # AdviceOrchestrator dispatches between standard (LLM) and agent mode at
    # runtime based on AnalysisMode.
    llm_service: LlmAnalysisService | None = None
    agent_orch: AgentOrchestrator | None = None

    if client is not None:
        if ai_settings is None:
            raise ValueError("ai_settings required when client is provided")
        llm_service = LlmAnalysisService(client, ai_settings, thresholds)
        agent_orch = AgentOrchestrator(client, static, ai_settings)

    return AdviceOrchestrator(static, llm_service, agent_orch)


@contextmanager
def create_analysis_context(
    *,
    mode: AnalysisMode = AnalysisMode.AI,
    model: str = DEFAULT_MODEL,
    thresholds: Thresholds | None = None,
) -> Iterator[AdviceOrchestrator]:
    from spark_advisor_models.config import AiSettings

    resolved_thresholds = thresholds or DEFAULT_THRESHOLDS
    client = None
    ai_settings: AiSettings | None = None

    if mode in (AnalysisMode.AI, AnalysisMode.AGENT):
        from spark_advisor_analyzer.ai.client import AnthropicClient

        ai_settings = AiSettings(model=model)
        client = AnthropicClient(timeout=ai_settings.api_timeout)
        client.open()

    try:
        yield create_analysis_stack(
            client=client,
            ai_settings=ai_settings,
            thresholds=resolved_thresholds,
        )
    finally:
        if client is not None:
            client.close()
