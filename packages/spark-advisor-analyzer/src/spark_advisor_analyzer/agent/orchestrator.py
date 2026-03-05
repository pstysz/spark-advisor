import json
import logging
from typing import Any

from anthropic.types import (
    Message,
    MessageParam,
    ToolChoiceAutoParam,
    ToolChoiceToolParam,
    ToolResultBlockParam,
    ToolUseBlock,
)
from pydantic import ValidationError

from spark_advisor_analyzer.agent.context import AgentContext
from spark_advisor_analyzer.agent.handlers import ToolExecutionError, execute_tool
from spark_advisor_analyzer.agent.prompts import build_agent_system_prompt, build_initial_message
from spark_advisor_analyzer.agent.tools import AGENT_TOOLS, AgentToolName
from spark_advisor_analyzer.ai.client import AnthropicClient
from spark_advisor_analyzer.ai.report_builder import build_advisor_report
from spark_advisor_models.config import AiSettings
from spark_advisor_models.model import AnalysisResult, AnalysisToolInput, JobAnalysis
from spark_advisor_rules import StaticAnalysisService

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    def __init__(
        self,
        client: AnthropicClient,
        static_analysis: StaticAnalysisService,
        ai_settings: AiSettings,
    ) -> None:
        self._client = client
        self._static = static_analysis
        self._ai = ai_settings
        self._max_iterations = ai_settings.max_agent_iterations
        self._system_prompt = build_agent_system_prompt(self._max_iterations)

    def run(self, job: JobAnalysis) -> AnalysisResult:
        context = AgentContext(job=job)
        messages: list[MessageParam] = [
            MessageParam(role="user", content=build_initial_message(job)),
        ]

        for iteration in range(self._max_iterations):
            logger.info("Agent iteration %d/%d", iteration + 1, self._max_iterations)

            response = self._call_claude(messages, tool_choice=ToolChoiceAutoParam(type="auto"))

            final_input = self._extract_final_report(response)
            if final_input is not None:
                logger.info("Agent completed after %d iterations", iteration + 1)
                return self._build_result(job, context, final_input)

            tool_uses = [b for b in response.content if isinstance(b, ToolUseBlock)]

            if not tool_uses:
                messages.append(MessageParam(role="assistant", content=response.content))
                messages.append(
                    MessageParam(
                        role="user",
                        content=f"Please call {AgentToolName.SUBMIT_FINAL_REPORT} with your analysis now.",
                    )
                )
                continue

            messages.append(MessageParam(role="assistant", content=response.content))

            tool_results: list[ToolResultBlockParam] = []
            for tool_use in tool_uses:
                logger.info("Executing tool: %s", tool_use.name)
                result_content = self._execute_single_tool(tool_use.name, tool_use.input, context)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use.id,
                        "content": result_content,
                    }
                )

            messages.append(MessageParam(role="user", content=tool_results))

        logger.warning("Agent hit max iterations (%d), forcing final report", self._max_iterations)
        return self._force_final_report(job, context, messages)

    def _call_claude(
        self,
        messages: list[MessageParam],
        *,
        tool_choice: ToolChoiceAutoParam | ToolChoiceToolParam,
    ) -> Message:
        return self._client.create_message(
            model=self._ai.model,
            max_tokens=self._ai.max_tokens,
            system=self._system_prompt,
            messages=messages,
            tools=AGENT_TOOLS,
            tool_choice=tool_choice,
        )

    def _execute_single_tool(
        self,
        name: str,
        input_data: Any,
        context: AgentContext,
    ) -> str:
        try:
            return execute_tool(name, input_data, context, self._static)
        except ToolExecutionError as e:
            logger.warning("Tool %s failed: %s", name, e)
            return json.dumps({"error": str(e)})
        except Exception:
            logger.exception("Unexpected error in tool %s", name)
            return json.dumps({"error": f"Internal error executing {name}"})

    def _force_final_report(
        self,
        job: JobAnalysis,
        context: AgentContext,
        messages: list[MessageParam],
    ) -> AnalysisResult:
        messages.append(
            MessageParam(
                role="user",
                content=(
                    "You have reached the maximum number of tool calls. "
                    "You MUST call submit_final_report now with your best analysis "
                    "based on the information gathered so far."
                ),
            )
        )

        response = self._call_claude(
            messages,
            tool_choice=ToolChoiceToolParam(type="tool", name=AgentToolName.SUBMIT_FINAL_REPORT),
        )

        final_input = self._extract_final_report(response)
        if final_input is not None:
            return self._build_result(job, context, final_input)

        logger.error("Force-submit failed, returning rules-only result")
        rule_results = context.get_or_run_rules(self._static)
        return AnalysisResult(app_id=job.app_id, job=job, rule_results=rule_results)

    def _build_result(
        self,
        job: JobAnalysis,
        context: AgentContext,
        parsed: AnalysisToolInput,
    ) -> AnalysisResult:
        rule_results = context.get_or_run_rules(self._static)
        report = build_advisor_report(job.app_id, parsed, rule_results)
        return AnalysisResult(app_id=job.app_id, job=job, rule_results=rule_results, ai_report=report)

    @staticmethod
    def _extract_final_report(response: Message) -> AnalysisToolInput | None:
        for block in response.content:
            if isinstance(block, ToolUseBlock) and block.name == AgentToolName.SUBMIT_FINAL_REPORT:
                try:
                    return AnalysisToolInput.model_validate(block.input)
                except ValidationError:
                    logger.warning("Failed to validate submit_final_report input, skipping")
                    return None
        return None
