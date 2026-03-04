from typing import Any
from unittest.mock import MagicMock

from anthropic.types import Message, TextBlock, ToolUseBlock, Usage
from factories import make_job

from spark_advisor_analyzer.agent.context import AgentContext
from spark_advisor_analyzer.agent.orchestrator import AgentOrchestrator
from spark_advisor_analyzer.agent.tools import AgentToolName
from spark_advisor_models.config import AiSettings
from spark_advisor_rules import StaticAnalysisService, default_rules


def make_agent_context(**overrides: object) -> AgentContext:
    return AgentContext(job=make_job(**overrides))


def make_default_static() -> StaticAnalysisService:
    return StaticAnalysisService(default_rules())


def make_tool_use_message(tool_name: str, tool_input: dict[str, Any], tool_id: str = "toolu_1") -> Message:
    return Message(
        id="msg_test",
        type="message",
        role="assistant",
        model="claude-sonnet-4-6",
        content=[ToolUseBlock(id=tool_id, type="tool_use", name=tool_name, input=tool_input)],
        stop_reason="tool_use",
        usage=Usage(input_tokens=100, output_tokens=200, cache_creation_input_tokens=0, cache_read_input_tokens=0),
    )


def make_final_report_message(tool_id: str = "toolu_final") -> Message:
    return make_tool_use_message(
        AgentToolName.SUBMIT_FINAL_REPORT,
        {
            "summary": "Job has minor issues.",
            "severity": "warning",
            "recommendations": [
                {
                    "priority": 1,
                    "title": "Enable AQE",
                    "parameter": "spark.sql.adaptive.skewJoin.enabled",
                    "current_value": "false",
                    "recommended_value": "true",
                    "explanation": "Fixes skew automatically.",
                    "estimated_impact": "~40% faster Stage 0",
                    "risk": "Minor overhead.",
                }
            ],
            "causal_chain": "",
        },
        tool_id,
    )


def make_text_message() -> Message:
    return Message(
        id="msg_text",
        type="message",
        role="assistant",
        model="claude-sonnet-4-6",
        content=[TextBlock(type="text", text="Let me analyze this job...")],
        stop_reason="end_turn",
        usage=Usage(input_tokens=100, output_tokens=50, cache_creation_input_tokens=0, cache_read_input_tokens=0),
    )


def make_agent_orchestrator(mock_client: MagicMock) -> AgentOrchestrator:
    return AgentOrchestrator(
        client=mock_client,
        static_analysis=StaticAnalysisService(default_rules()),
        ai_settings=AiSettings(),
    )
