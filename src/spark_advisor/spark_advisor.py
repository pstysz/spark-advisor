from spark_advisor.ai.llm_service import LlmService
from spark_advisor.ai.rules import apply_static_rules
from spark_advisor.config import DEFAULT_MODEL
from spark_advisor.model import AnalysisResult
from spark_advisor.model.metrics import JobAnalysis


class SparkAdvisor:
    def __init__(self, llm_service: LlmService | None = None) -> None:
        self._llm = llm_service

    def run(
        self,
        job: JobAnalysis,
        ai_enabled: bool = False,
        model: str = DEFAULT_MODEL,
    ) -> AnalysisResult:
        ai_report = None
        rule_results = apply_static_rules(job)
        if ai_enabled and rule_results:
            if self._llm is None:
                raise RuntimeError("LlmService is required for AI analysis")
            ai_report = self._llm.analyze(job, rule_results, model)

        return AnalysisResult(
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )
