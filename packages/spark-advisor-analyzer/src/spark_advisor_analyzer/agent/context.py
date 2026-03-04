from dataclasses import dataclass, field

from spark_advisor_models.model import JobAnalysis, RuleResult


@dataclass
class AgentContext:
    job: JobAnalysis
    rule_results: list[RuleResult] = field(default_factory=list)
    rules_executed: bool = False
