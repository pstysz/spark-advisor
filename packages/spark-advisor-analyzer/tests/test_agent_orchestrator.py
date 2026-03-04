from unittest.mock import MagicMock

from anthropic.types import Message, TextBlock, ToolUseBlock, Usage
from factories import make_job

from agent_factories import make_agent_orchestrator, make_final_report_message, make_text_message, make_tool_use_message
from spark_advisor_analyzer.agent.orchestrator import MAX_ITERATIONS
from spark_advisor_analyzer.agent.tools import AgentToolName
from spark_advisor_models.model import Severity


class TestAgentOrchestrator:
    def test_two_turn_flow(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [
            make_tool_use_message(AgentToolName.GET_JOB_OVERVIEW, {}),
            make_final_report_message(),
        ]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert result.ai_report.severity == Severity.WARNING
        assert len(result.ai_report.recommendations) == 1
        assert mock_client.create_message.call_count == 2

    def test_three_turn_with_rules_and_stage(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [
            make_tool_use_message(AgentToolName.RUN_RULES_ENGINE, {}, "toolu_1"),
            make_tool_use_message(AgentToolName.GET_STAGE_DETAILS, {"stage_id": 0}, "toolu_2"),
            make_final_report_message("toolu_3"),
        ]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == 3
        assert len(result.rule_results) > 0

    def test_text_only_response_gets_nudge(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [
            make_text_message(),
            make_final_report_message(),
        ]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == 2

    def test_max_iterations_forces_report(self) -> None:
        overview_msg = make_tool_use_message(AgentToolName.GET_JOB_OVERVIEW, {})
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [overview_msg] * MAX_ITERATIONS + [
            make_final_report_message()
        ]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == MAX_ITERATIONS + 1

    def test_tool_error_sent_back_to_claude(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [
            make_tool_use_message(AgentToolName.GET_STAGE_DETAILS, {"stage_id": 999}),
            make_final_report_message(),
        ]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        second_call_messages = mock_client.create_message.call_args_list[1].kwargs["messages"]
        last_msg = second_call_messages[-1]
        assert "error" in str(last_msg)

    def test_submit_final_report_terminates_immediately(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [make_final_report_message()]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == 1

    def test_rules_only_fallback_on_force_failure(self) -> None:
        overview_msg = make_tool_use_message(AgentToolName.GET_JOB_OVERVIEW, {})
        empty_response = Message(
            id="msg_empty",
            type="message",
            role="assistant",
            model="claude-sonnet-4-6",
            content=[TextBlock(type="text", text="I cannot provide analysis.")],
            stop_reason="end_turn",
            usage=Usage(
                input_tokens=100, output_tokens=50, cache_creation_input_tokens=0, cache_read_input_tokens=0,
            ),
        )
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [overview_msg] * MAX_ITERATIONS + [empty_response]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is None
        assert len(result.rule_results) > 0

    def test_suggested_config_extracted(self) -> None:
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [make_final_report_message()]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert "spark.sql.adaptive.skewJoin.enabled" in result.ai_report.suggested_config

    def test_multiple_tools_in_single_response(self) -> None:
        multi_tool_msg = Message(
            id="msg_test",
            type="message",
            role="assistant",
            model="claude-sonnet-4-6",
            content=[
                ToolUseBlock(id="t1", type="tool_use", name=AgentToolName.GET_JOB_OVERVIEW, input={}),
                ToolUseBlock(id="t2", type="tool_use", name=AgentToolName.RUN_RULES_ENGINE, input={}),
            ],
            stop_reason="tool_use",
            usage=Usage(input_tokens=100, output_tokens=200, cache_creation_input_tokens=0, cache_read_input_tokens=0),
        )
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [multi_tool_msg, make_final_report_message()]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == 2

    def test_malformed_final_report_triggers_fallback(self) -> None:
        malformed = make_tool_use_message(
            AgentToolName.SUBMIT_FINAL_REPORT,
            {"summary": "ok"},
        )
        mock_client = MagicMock()
        mock_client.create_message.side_effect = [malformed, make_final_report_message()]

        orch = make_agent_orchestrator(mock_client)
        result = orch.run(make_job())

        assert result.ai_report is not None
        assert mock_client.create_message.call_count == 2
