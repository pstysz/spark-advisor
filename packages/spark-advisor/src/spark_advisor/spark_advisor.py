from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor.config import DEFAULT_MODEL
from spark_advisor.model import AnalysisResult
from spark_advisor.model.metrics import JobAnalysis


class SparkAdvisor:
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
        model: str = DEFAULT_MODEL,
    ) -> AnalysisResult:
        ai_report = None
        rule_results = self._static.analyze(job)
        if self._llm:
            ai_report = self._llm.analyze(job, rule_results, model)

        return AnalysisResult(
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
