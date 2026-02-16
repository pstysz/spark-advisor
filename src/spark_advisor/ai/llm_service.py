import json
import os
from typing import Any

import anthropic
from anthropic.types import MessageParam, TextBlock

from spark_advisor.ai.prompts import SYSTEM_PROMPT, build_user_message
from spark_advisor.analysis.config import DEFAULT_MAX_TOKENS, DEFAULT_MODEL
from spark_advisor.core import (
    AdvisorReport,
    JobAnalysis,
    Recommendation,
    RuleResult,
    Severity,
)


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
            system=SYSTEM_PROMPT,
            messages=[MessageParam(role="user", content=user_message)],
        )

        first_block = response.content[0]
        assert isinstance(first_block, TextBlock)
        response_text = first_block.text
        parsed = self._parse_ai_response(response_text)

        return AdvisorReport(
            app_id=job.app_id,
            summary=parsed.get("summary", ""),
            severity=Severity(parsed.get("severity", "info")),
            rule_results=rule_results,
            recommendations=[Recommendation(**rec) for rec in parsed.get("recommendations", [])],
            causal_chain=parsed.get("causal_chain", ""),
            suggested_config=self._extract_suggested_config(parsed),
        )

    @staticmethod
    def _parse_ai_response(text: str) -> dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]

        return json.loads(cleaned.strip())  # type: ignore[no-any-return]

    @staticmethod
    def _extract_suggested_config(parsed: dict[str, Any]) -> dict[str, str]:
        config: dict[str, str] = {}
        for rec in parsed.get("recommendations", []):
            param = rec.get("parameter", "")
            value = rec.get("recommended_value", "")
            if param.startswith("spark.") and value:
                config[param] = value
        return config
