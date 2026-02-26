from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor_models.model import AnalysisResult, JobAnalysis


class AdviceOrchestrator:
    def __init__(
            self,
            static_analysis: StaticAnalysisService,
            llm_service: LlmAnalysisService | None = None,
    ) -> None:
        self._static = static_analysis
        self._llm = llm_service

    def run(
            self,
            job: JobAnalysis,
    ) -> AnalysisResult:
        ai_report = None
        rule_results = self._static.analyze(job)
        if self._llm:
            ai_report = self._llm.analyze(job, rule_results)

        return AnalysisResult(
            app_id=job.app_id,
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
