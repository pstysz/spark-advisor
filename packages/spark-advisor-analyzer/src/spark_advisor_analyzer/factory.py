from __future__ import annotations

from typing import TYPE_CHECKING

from spark_advisor_analyzer.agent.orchestrator import AgentOrchestrator
from spark_advisor_analyzer.ai.service import LlmAnalysisService
from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

if TYPE_CHECKING:
    from spark_advisor_analyzer.ai.client import AnthropicClient
    from spark_advisor_models.config import AiSettings, Thresholds


def create_analysis_stack(
    *,
    client: AnthropicClient | None,
    ai_settings: AiSettings | None = None,
    thresholds: Thresholds,
) -> AdviceOrchestrator:
    static = StaticAnalysisService(rules_for_threshold(thresholds))

    llm_service: LlmAnalysisService | None = None
    agent_orch: AgentOrchestrator | None = None

    if client is not None:
        if ai_settings is None:
            raise ValueError("ai_settings required when client is provided")
        llm_service = LlmAnalysisService(client, ai_settings, thresholds)
        agent_orch = AgentOrchestrator(client, static, ai_settings)

    return AdviceOrchestrator(static, llm_service, agent_orch)
