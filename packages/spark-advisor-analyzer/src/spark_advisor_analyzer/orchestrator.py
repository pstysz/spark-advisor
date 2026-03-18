from __future__ import annotations

from typing import TYPE_CHECKING

from spark_advisor_models.model import AnalysisMode, AnalysisResult, JobAnalysis
from spark_advisor_models.tracing import get_tracer

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

    def run(self, job: JobAnalysis, *, mode: AnalysisMode = AnalysisMode.AI) -> AnalysisResult:
        tracer = get_tracer()
        if mode == AnalysisMode.AGENT:
            if self._agent is None:
                raise ValueError("Agent mode requested but no AgentOrchestrator was configured")
            with tracer.start_as_current_span("analyzer.agent", attributes={"app_id": job.app_id}):
                return self._agent.run(job)

        if mode == AnalysisMode.AI and self._llm is None:
            raise ValueError("AI mode requested but no LlmAnalysisService was configured")

        with tracer.start_as_current_span("analyzer.rules_engine", attributes={"app_id": job.app_id}):
            rule_results = self._static.analyze(job)

        ai_report = None
        if self._llm and mode != AnalysisMode.STATIC:
            with tracer.start_as_current_span("analyzer.ai_analysis", attributes={"app_id": job.app_id}):
                ai_report = self._llm.analyze(job, rule_results)

        return AnalysisResult(
            app_id=job.app_id,
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
