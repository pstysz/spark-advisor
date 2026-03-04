from __future__ import annotations

from typing import TYPE_CHECKING

from spark_advisor_models.model import AnalysisResult, JobAnalysis
from spark_advisor_models.model.output import AnalysisMode

if TYPE_CHECKING:
    from spark_advisor_analyzer.agent.orchestrator import AgentOrchestrator
    from spark_advisor_analyzer.ai.service import LlmAnalysisService
    from spark_advisor_rules import StaticAnalysisService


class AdviceOrchestrator:
    def __init__(
            self,
            static_analysis: StaticAnalysisService,
            llm_service: LlmAnalysisService | None = None,
            agent: AgentOrchestrator | None = None,
    ) -> None:
        self._static = static_analysis
        self._llm = llm_service
        self._agent = agent

    def run(self, job: JobAnalysis, *, mode: AnalysisMode = AnalysisMode.STANDARD) -> AnalysisResult:
        if mode == AnalysisMode.AGENT:
            if self._agent is None:
                raise ValueError("Agent mode requested but no AgentOrchestrator was configured")
            return self._agent.run(job)

        rule_results = self._static.analyze(job)
        ai_report = self._llm.analyze(job, rule_results) if self._llm else None
        return AnalysisResult(
            app_id=job.app_id,
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
