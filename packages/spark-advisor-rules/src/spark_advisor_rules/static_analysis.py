from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import JobAnalysis, RuleResult
from spark_advisor_rules.rules import Rule, rules_for_threshold


class StaticAnalysisService:
    def __init__(self, rules: list[Rule] | None = None, *, thresholds: Thresholds | None = None) -> None:
        if rules is not None and thresholds is not None:
            raise ValueError("Provide either 'rules' or 'thresholds', not both")
        if rules is not None:
            self._rules = rules
        else:
            from spark_advisor_models.defaults import DEFAULT_THRESHOLDS

            self._rules = rules_for_threshold(thresholds or DEFAULT_THRESHOLDS)

    def analyze(self, job: JobAnalysis) -> list[RuleResult]:
        all_results: list[RuleResult] = []
        for rule in self._rules:
            all_results.extend(rule.evaluate(job))

        all_results.sort(key=lambda r: r.severity.order)

        return all_results
