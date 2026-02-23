from unittest.mock import MagicMock

import pytest
from anthropic.types import Message, ToolUseBlock, Usage

from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.model import Severity
from tests.factories import make_job, make_rule_result


def _fake_tool_response(tool_input: dict) -> Message:
    return Message(
        id="msg_test",
        type="message",
        role="assistant",
        model="claude-sonnet-4-20250514",
        content=[
            ToolUseBlock(
                id="toolu_test",
                type="tool_use",
                name="submit_analysis",
                input=tool_input,
            )
        ],
        stop_reason="tool_use",
        usage=Usage(input_tokens=100, output_tokens=200),
    )


def _make_service(tool_input: dict) -> LlmAnalysisService:
    mock_client = MagicMock()
    mock_client.create_message.return_value = _fake_tool_response(tool_input)
    return LlmAnalysisService(mock_client)


class TestLlmAnalysisService:
    def test_analyze_returns_advisor_report(self) -> None:
        service = _make_service(
            {
                "summary": "Data skew detected in Stage 0.",
                "severity": "warning",
                "recommendations": [
                    {
                        "priority": 1,
                        "title": "Enable AQE skew join",
                        "parameter": "spark.sql.adaptive.skewJoin.enabled",
                        "current_value": "false",
                        "recommended_value": "true",
                        "explanation": "AQE will split skewed partitions automatically.",
                        "estimated_impact": "~40% reduction in Stage 0 duration",
                        "risk": "Minor scheduling overhead.",
                    }
                ],
                "causal_chain": "Skew in Stage 0 causes uneven task distribution.",
            }
        )

        report = service.analyze(make_job(), [make_rule_result()])

        assert report.app_id == "app-test-001"
        assert report.severity == Severity.WARNING
        assert report.summary == "Data skew detected in Stage 0."
        assert len(report.recommendations) == 1
        assert report.recommendations[0].title == "Enable AQE skew join"
        assert report.causal_chain == "Skew in Stage 0 causes uneven task distribution."

    def test_suggested_config_extracted(self) -> None:
        service = _make_service(
            {
                "summary": "Multiple issues found.",
                "severity": "critical",
                "recommendations": [
                    {
                        "priority": 1,
                        "title": "Increase shuffle partitions",
                        "parameter": "spark.sql.shuffle.partitions",
                        "current_value": "200",
                        "recommended_value": "800",
                        "explanation": "Too few partitions.",
                        "estimated_impact": "~50% faster",
                        "risk": "More tasks.",
                    },
                    {
                        "priority": 2,
                        "title": "Salt join keys",
                        "parameter": "code_change",
                        "current_value": "N/A",
                        "recommended_value": "Add salt prefix to join key",
                        "explanation": "Distributes skewed keys.",
                        "estimated_impact": "~30% faster",
                        "risk": "Code complexity.",
                    },
                ],
                "causal_chain": "",
            }
        )

        report = service.analyze(make_job(), [make_rule_result()])

        assert report.suggested_config == {"spark.sql.shuffle.partitions": "800"}
        assert len(report.recommendations) == 2

    def test_empty_recommendations(self) -> None:
        service = _make_service(
            {
                "summary": "Job looks healthy.",
                "severity": "info",
                "recommendations": [],
                "causal_chain": "",
            }
        )

        report = service.analyze(make_job(), [])

        assert report.severity == Severity.INFO
        assert report.recommendations == []
        assert report.suggested_config == {}

    def test_extract_tool_input_raises_on_missing_tool(self) -> None:
        with pytest.raises(ValueError, match="did not call submit_analysis"):
            LlmAnalysisService._extract_tool_input([])
