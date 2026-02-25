from typing import Any

from anthropic.types import MessageParam, ToolChoiceToolParam, ToolUseBlock

from spark_advisor.ai.config import ANALYSIS_TOOL
from spark_advisor.ai.prompts_builder import build_system_prompt, build_user_message
from spark_advisor.api.anthropic_client import AnthropicClient
from spark_advisor.config import AiSettings, Thresholds
from spark_advisor.model import AdvisorReport, AnalysisToolInput, Recommendation, RuleResult, Severity
from spark_advisor_shared.model.metrics import JobAnalysis


class LlmAnalysisService:
    def __init__(self, client: AnthropicClient, ai: AiSettings, thresholds: Thresholds) -> None:
        self._client = client
        self._ai_settings = ai
        self._thresholds = thresholds
        self._prompt = build_system_prompt(self._thresholds)

    def analyze(
            self,
            job: JobAnalysis,
            rule_results: list[RuleResult],
    ) -> AdvisorReport:
        user_message = build_user_message(job, rule_results, self._thresholds)
        response = self._client.create_message(
            model=self._ai_settings.model,
            max_tokens=self._ai_settings.max_tokens,
            system=self._prompt,
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
