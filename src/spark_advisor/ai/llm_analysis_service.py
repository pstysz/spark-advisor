from typing import Any

from anthropic.types import MessageParam, ToolChoiceToolParam, ToolUseBlock

from spark_advisor.ai.config import ANALYSIS_TOOL
from spark_advisor.ai.prompts_builder import SYSTEM_PROMPT, build_user_message
from spark_advisor.api.anthropic_client import AnthropicClient
from spark_advisor.config import DEFAULT_MAX_TOKENS, DEFAULT_MODEL
from spark_advisor.model import AdvisorReport, AnalysisToolInput, Recommendation, RuleResult, Severity
from spark_advisor.model.metrics import JobAnalysis


class LlmAnalysisService:
    def __init__(self, client: AnthropicClient) -> None:
        self._client = client

    def analyze(
        self,
        job: JobAnalysis,
        rule_results: list[RuleResult],
        model: str = DEFAULT_MODEL,
    ) -> AdvisorReport:
        user_message = build_user_message(job, rule_results)
        response = self._client.create_message(
            model=model,
            max_tokens=DEFAULT_MAX_TOKENS,
            system=SYSTEM_PROMPT,
            messages=[MessageParam(role="user", content=user_message)],
            tools=[ANALYSIS_TOOL],
            tool_choice=ToolChoiceToolParam(type="tool", name="submit_analysis"),
        )

        raw_input = self._extract_tool_input(response.content)
        parsed = AnalysisToolInput.model_validate(raw_input)

        return AdvisorReport(
            app_id=job.app_id,
            summary=parsed.summary,
            severity=Severity(parsed.severity.upper()),
            rule_results=rule_results,
            recommendations=[Recommendation(**rec.model_dump()) for rec in parsed.recommendations],
            causal_chain=parsed.causal_chain,
            suggested_config=self._extract_suggested_config(parsed),
        )

    @staticmethod
    def _extract_tool_input(content: Any) -> dict[str, Any]:
        for block in content:
            if isinstance(block, ToolUseBlock) and block.name == "submit_analysis":
                return block.input
        raise ValueError("Model did not call submit_analysis tool")

    @staticmethod
    def _extract_suggested_config(parsed: AnalysisToolInput) -> dict[str, str]:
        return {
            rec.parameter: rec.recommended_value
            for rec in parsed.recommendations
            if rec.parameter.startswith("spark.") and rec.recommended_value
        }
