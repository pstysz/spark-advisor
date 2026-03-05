from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import JobAnalysis, RuleResult
from spark_advisor_rules.rules import Rule, rules_for_threshold


class StaticAnalysisService:
    def __init__(self, rules: list[Rule] | None = None) -> None:
        self._rules = rules if rules is not None else rules_for_threshold(Thresholds())

    def analyze(self, job: JobAnalysis) -> list[RuleResult]:
        all_results: list[RuleResult] = []
        for rule in self._rules:
            all_results.extend(rule.evaluate(job))

        all_results.sort(key=lambda r: r.severity.order)

        return all_results
