from unittest.mock import MagicMock, patch

import pytest
from anthropic.types import Message, ToolUseBlock, Usage

from spark_advisor.ai.llm_service import LlmService
from spark_advisor.core import (
    JobAnalysis,
    RuleResult,
    Severity,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
)


def _make_job() -> JobAnalysis:
    return JobAnalysis(
        app_id="app-test-001",
        app_name="TestJob",
        duration_ms=300_000,
        config=SparkConfig(raw={"spark.executor.memory": "4g"}),
        stages=[
            StageMetrics(
                stage_id=0,
                stage_name="Stage 0",
                duration_ms=100_000,
                tasks=TaskMetrics(
                    task_count=100,
                    median_duration_ms=1000,
                    max_duration_ms=5000,
                    min_duration_ms=500,
                    total_gc_time_ms=5000,
                ),
            )
        ],
    )


def _make_rule_results() -> list[RuleResult]:
    return [
        RuleResult(
            rule_id="data_skew",
            severity=Severity.WARNING,
            title="Data skew in Stage 0",
            message="Max task duration is 5.0x the median",
        )
    ]


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


class TestLlmService:
    def test_requires_api_key(self) -> None:
        with patch.dict("os.environ", {}, clear=True), pytest.raises(
            ValueError, match="ANTHROPIC_API_KEY"
        ):
            LlmService()

    def test_must_be_used_in_with_block(self) -> None:
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            service = LlmService()
            with pytest.raises(RuntimeError, match="within 'with' block"):
                service._get_client()

    @patch("spark_advisor.ai.llm_service.anthropic.Anthropic")
    def test_analyze_returns_advisor_report(self, mock_anthropic_cls: MagicMock) -> None:
        tool_input = {
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
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _fake_tool_response(tool_input)
        mock_anthropic_cls.return_value = mock_client

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}), LlmService() as service:
                report = service.analyze(_make_job(), _make_rule_results())

        assert report.app_id == "app-test-001"
        assert report.severity == Severity.WARNING
        assert report.summary == "Data skew detected in Stage 0."
        assert len(report.recommendations) == 1
        assert report.recommendations[0].title == "Enable AQE skew join"
        assert report.causal_chain == "Skew in Stage 0 causes uneven task distribution."

    @patch("spark_advisor.ai.llm_service.anthropic.Anthropic")
    def test_suggested_config_extracted(self, mock_anthropic_cls: MagicMock) -> None:
        tool_input = {
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
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _fake_tool_response(tool_input)
        mock_anthropic_cls.return_value = mock_client

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}), LlmService() as service:
                report = service.analyze(_make_job(), _make_rule_results())

        assert report.suggested_config == {"spark.sql.shuffle.partitions": "800"}
        assert len(report.recommendations) == 2

    @patch("spark_advisor.ai.llm_service.anthropic.Anthropic")
    def test_empty_recommendations(self, mock_anthropic_cls: MagicMock) -> None:
        tool_input = {
            "summary": "Job looks healthy.",
            "severity": "info",
            "recommendations": [],
            "causal_chain": "",
        }
        mock_client = MagicMock()
        mock_client.messages.create.return_value = _fake_tool_response(tool_input)
        mock_anthropic_cls.return_value = mock_client

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}), LlmService() as service:
                report = service.analyze(_make_job(), [])

        assert report.severity == Severity.INFO
        assert report.recommendations == []
        assert report.suggested_config == {}

    def test_extract_tool_input_raises_on_missing_tool(self) -> None:
        with pytest.raises(ValueError, match="did not call submit_analysis"):
            LlmService._extract_tool_input([])
