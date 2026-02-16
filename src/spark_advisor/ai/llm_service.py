import os
from typing import Any

import anthropic
from anthropic.types import MessageParam, ToolChoiceToolParam, ToolParam, ToolUseBlock

from spark_advisor.ai.prompts import build_system_prompt, build_user_message
from spark_advisor.analysis.config import DEFAULT_MAX_TOKENS, DEFAULT_MODEL
from spark_advisor.core import (
    AdvisorReport,
    JobAnalysis,
    Recommendation,
    RuleResult,
    Severity,
)

ANALYSIS_TOOL: ToolParam = {
    "name": "submit_analysis",
    "description": "Submit the Spark job analysis results.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "1-2 sentence overview of the job's health and key issues.",
            },
            "severity": {
                "type": "string",
                "enum": ["critical", "warning", "info"],
                "description": "Overall severity: critical, warning, or info.",
            },
            "recommendations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "priority": {
                            "type": "integer",
                            "description": "Priority rank (1 = highest impact).",
                        },
                        "title": {
                            "type": "string",
                            "description": "Short title for the recommendation.",
                        },
                        "parameter": {
                            "type": "string",
                            "description": (
                                "Spark config parameter (e.g. spark.sql.shuffle.partitions). "
                                'Use "code_change" for non-config recommendations.'
                            ),
                        },
                        "current_value": {
                            "type": "string",
                            "description": "Current value of the parameter.",
                        },
                        "recommended_value": {
                            "type": "string",
                            "description": "Recommended new value.",
                        },
                        "explanation": {
                            "type": "string",
                            "description": "Brief explanation of the mechanism (2-3 sentences).",
                        },
                        "estimated_impact": {
                            "type": "string",
                            "description": (
                                "Quantified estimate "
                                '(e.g. "~30% reduction in Stage 4 duration").'
                            ),
                        },
                        "risk": {
                            "type": "string",
                            "description": "Potential downsides of this change.",
                        },
                    },
                    "required": [
                        "priority",
                        "title",
                        "parameter",
                        "current_value",
                        "recommended_value",
                        "explanation",
                        "estimated_impact",
                        "risk",
                    ],
                },
            },
            "causal_chain": {
                "type": "string",
                "description": "Description of how problems are related, if applicable.",
            },
        },
        "required": ["summary", "severity", "recommendations", "causal_chain"],
    },
}


def llm_client() -> "LlmService":
    return LlmService()


class LlmService:
    def __init__(self) -> None:
        self._api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not self._api_key:
            raise ValueError("ANTHROPIC_API_KEY variable is required for AI analysis")
        self._client: anthropic.Anthropic | None = None

    def __enter__(self) -> "LlmService":
        self._client = anthropic.Anthropic(api_key=self._api_key)
        return self

    def __exit__(self, *_: object) -> None:
        if self._client:
            self._client.close()

    def _get_client(self) -> anthropic.Anthropic:
        if self._client is None:
            raise RuntimeError("LlmService must be used within 'with' block")
        return self._client

    def analyze(
        self,
        job: JobAnalysis,
        rule_results: list[RuleResult],
        model: str = DEFAULT_MODEL,
    ) -> AdvisorReport:
        user_message = build_user_message(job, rule_results)

        response = self._get_client().messages.create(
            model=model,
            max_tokens=DEFAULT_MAX_TOKENS,
            system=build_system_prompt(),
            messages=[MessageParam(role="user", content=user_message)],
            tools=[ANALYSIS_TOOL],
            tool_choice=ToolChoiceToolParam(type="tool", name="submit_analysis"),
        )

        parsed = self._extract_tool_input(response.content)

        return AdvisorReport(
            app_id=job.app_id,
            summary=parsed.get("summary", ""),
            severity=Severity(parsed.get("severity", "info")),
            rule_results=rule_results,
            recommendations=[
                Recommendation(**rec) for rec in parsed.get("recommendations", [])
            ],
            causal_chain=parsed.get("causal_chain", ""),
            suggested_config=self._extract_suggested_config(parsed),
        )

    @staticmethod
    def _extract_tool_input(content: Any) -> dict[str, Any]:
        for block in content:
            if isinstance(block, ToolUseBlock) and block.name == "submit_analysis":
                return block.input
        raise ValueError("Model did not call submit_analysis tool")

    @staticmethod
    def _extract_suggested_config(parsed: dict[str, Any]) -> dict[str, str]:
        config: dict[str, str] = {}
        for rec in parsed.get("recommendations", []):
            param = rec.get("parameter", "")
            value = rec.get("recommended_value", "")
            if param.startswith("spark.") and value:
                config[param] = value
        return config
