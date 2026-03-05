from spark_advisor_analyzer.agent.prompts import build_agent_system_prompt, build_initial_message
from spark_advisor_models.config import AiSettings
from spark_advisor_models.testing import make_job

DEFAULT_MAX_ITERATIONS = AiSettings().max_agent_iterations


class TestAgentSystemPrompt:
    def test_includes_diagnostic_strategy(self) -> None:
        prompt = build_agent_system_prompt(DEFAULT_MAX_ITERATIONS)
        assert "get_job_overview" in prompt
        assert "run_rules_engine" in prompt
        assert "submit_final_report" in prompt

    def test_includes_recommendation_rules(self) -> None:
        prompt = build_agent_system_prompt(DEFAULT_MAX_ITERATIONS)
        assert "SPECIFIC config values" in prompt
        assert "at most 7" in prompt

    def test_includes_iteration_limit(self) -> None:
        prompt = build_agent_system_prompt(DEFAULT_MAX_ITERATIONS)
        assert f"maximum of {DEFAULT_MAX_ITERATIONS} tool calls" in prompt

    def test_custom_iteration_limit(self) -> None:
        prompt = build_agent_system_prompt(5)
        assert "maximum of 5 tool calls" in prompt


class TestBuildInitialMessage:
    def test_includes_job_info(self) -> None:
        msg = build_initial_message(make_job())
        assert "app-test-001" in msg
        assert "TestJob" in msg
        assert "min" in msg

    def test_includes_stage_count(self) -> None:
        msg = build_initial_message(make_job())
        assert "1 stages" in msg

    def test_includes_task_count(self) -> None:
        msg = build_initial_message(make_job())
        assert "100 tasks" in msg
