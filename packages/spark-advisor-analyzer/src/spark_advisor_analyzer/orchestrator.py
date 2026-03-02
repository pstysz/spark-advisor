from spark_advisor_analyzer.ai.service import LlmAnalysisService
from spark_advisor_models.model import AnalysisResult, JobAnalysis
from spark_advisor_rules import StaticAnalysisService


class AdviceOrchestrator:
    def __init__(
            self,
            static_analysis: StaticAnalysisService,
            llm_service: LlmAnalysisService | None = None,
    ) -> None:
        self._static = static_analysis
        self._llm = llm_service

    def run(self, job: JobAnalysis) -> AnalysisResult:
        rule_results = self._static.analyze(job)
        ai_report = self._llm.analyze(job, rule_results) if self._llm else None
        return AnalysisResult(
            app_id=job.app_id,
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
