from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spark_advisor_models.model import JobAnalysis, RuleResult
    from spark_advisor_rules import StaticAnalysisService


@dataclass
class AgentContext:
    job: JobAnalysis
    _rule_results: list[RuleResult] = field(default_factory=list, init=False)
    _rules_executed: bool = field(default=False, init=False)

    @property
    def rules_executed(self) -> bool:
        return self._rules_executed

    @property
    def rule_results(self) -> list[RuleResult]:
        return self._rule_results

    def get_or_run_rules(self, static: StaticAnalysisService) -> list[RuleResult]:
        if not self._rules_executed:
            self._rule_results = static.analyze(self.job)
            self._rules_executed = True
        return self._rule_results
