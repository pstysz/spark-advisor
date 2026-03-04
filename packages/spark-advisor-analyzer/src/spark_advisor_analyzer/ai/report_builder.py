from spark_advisor_models.model import (
    AdvisorReport,
    AnalysisToolInput,
    Recommendation,
    RuleResult,
    Severity,
)


def build_advisor_report(
    app_id: str,
    parsed: AnalysisToolInput,
    rule_results: list[RuleResult],
) -> AdvisorReport:
    return AdvisorReport(
        app_id=app_id,
        summary=parsed.summary,
        severity=Severity(parsed.severity.upper()),
        rule_results=rule_results,
        recommendations=[Recommendation(**rec.model_dump()) for rec in parsed.recommendations],
        causal_chain=parsed.causal_chain,
        suggested_config={
            rec.parameter: rec.recommended_value
            for rec in parsed.recommendations
            if rec.parameter.startswith("spark.") and rec.recommended_value
        },
    )
