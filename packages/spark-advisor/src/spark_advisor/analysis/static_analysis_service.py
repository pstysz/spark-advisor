from spark_advisor.analysis.rules import Rule, default_rules
from spark_advisor.model import RuleResult
from spark_advisor.model.metrics import JobAnalysis
from spark_advisor.model.output import Severity


class StaticAnalysisService:
    def __init__(self, rules: list[Rule] | None = None) -> None:
        self._rules = rules or default_rules()

    def analyze(self, job: JobAnalysis) -> list[RuleResult]:
        all_results: list[RuleResult] = []
        for rule in self._rules:
            all_results.extend(rule.evaluate(job))

        severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
        all_results.sort(key=lambda r: severity_order.get(r.severity, 99))

        return all_results
