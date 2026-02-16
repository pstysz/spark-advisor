from pydantic import BaseModel, ConfigDict, Field

from spark_advisor.core.models import JobAnalysis, Severity


class RuleResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    rule_id: str
    severity: Severity
    title: str
    message: str
    stage_id: int | None = None
    current_value: str = ""
    recommended_value: str = ""
    estimated_impact: str = ""


class Recommendation(BaseModel):
    model_config = ConfigDict(frozen=True)

    priority: int = 0
    title: str = ""
    parameter: str = ""
    current_value: str = ""
    recommended_value: str = ""
    explanation: str = ""
    estimated_impact: str = ""
    risk: str = ""


class AdvisorReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    app_id: str
    summary: str
    severity: Severity
    rule_results: list[RuleResult]
    recommendations: list[Recommendation]
    causal_chain: str = ""
    suggested_config: dict[str, str] = Field(default_factory=dict)


class AnalysisResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    job: JobAnalysis
    rule_results: list[RuleResult]
    ai_report: AdvisorReport | None = None
