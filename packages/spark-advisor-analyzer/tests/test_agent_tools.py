import json

import pytest

from agent_factories import make_agent_context, make_default_static
from spark_advisor_analyzer.agent.handlers import ToolExecutionError, execute_tool
from spark_advisor_analyzer.agent.tools import AgentToolName
from spark_advisor_models.testing import make_executors, make_stage


class TestGetJobOverview:
    def test_returns_valid_json(self) -> None:
        ctx, static = make_agent_context(), make_default_static()
        result = json.loads(execute_tool(AgentToolName.GET_JOB_OVERVIEW, {}, ctx, static))
        assert result["app_id"] == "app-test-001"
        assert result["stage_count"] == 1
        assert "config" in result
        assert "stages_summary" in result

    def test_includes_executor_info_when_present(self) -> None:
        ctx = make_agent_context(executors=make_executors())
        result = json.loads(execute_tool(AgentToolName.GET_JOB_OVERVIEW, {}, ctx, make_default_static()))
        assert "executors" in result
        assert result["executors"]["count"] == 10

    def test_stage_summary_includes_flags(self) -> None:
        ctx = make_agent_context(stages=[make_stage(0, spill_to_disk_bytes=1024 * 1024 * 1024)])
        result = json.loads(execute_tool(AgentToolName.GET_JOB_OVERVIEW, {}, ctx, make_default_static()))
        assert result["stages_summary"][0]["has_spill"] is True


class TestGetStageDetails:
    def test_returns_stage_metrics(self) -> None:
        ctx = make_agent_context(stages=[make_stage(0), make_stage(1)])
        result = json.loads(execute_tool(AgentToolName.GET_STAGE_DETAILS, {"stage_id": 1}, ctx, make_default_static()))
        assert result["stage_id"] == 1

    def test_invalid_stage_raises_error(self) -> None:
        with pytest.raises(ToolExecutionError, match="Stage 99 not found"):
            execute_tool(AgentToolName.GET_STAGE_DETAILS, {"stage_id": 99}, make_agent_context(), make_default_static())

    def test_missing_stage_id_raises_error(self) -> None:
        with pytest.raises(ToolExecutionError, match="Invalid input"):
            execute_tool(AgentToolName.GET_STAGE_DETAILS, {}, make_agent_context(), make_default_static())


class TestRunRulesEngine:
    def test_returns_findings(self) -> None:
        ctx, static = make_agent_context(), make_default_static()
        result = json.loads(execute_tool(AgentToolName.RUN_RULES_ENGINE, {}, ctx, static))
        assert "findings_count" in result
        assert isinstance(result["findings"], list)

    def test_caches_results(self) -> None:
        ctx = make_agent_context()
        static = make_default_static()
        execute_tool(AgentToolName.RUN_RULES_ENGINE, {}, ctx, static)
        assert ctx.rules_executed is True
        first_results = ctx.rule_results

        execute_tool(AgentToolName.RUN_RULES_ENGINE, {}, ctx, static)
        assert ctx.rule_results is first_results


class TestCalculateOptimalPartitions:
    def test_calculates_correctly(self) -> None:
        ctx = make_agent_context(config={"spark.sql.shuffle.partitions": "200"})
        result = json.loads(
            execute_tool(
                AgentToolName.CALCULATE_OPTIMAL_PARTITIONS,
                {"total_shuffle_bytes": 128 * 100 * 1024 * 1024},
                ctx,
                make_default_static(),
            )
        )
        assert result["optimal_partitions"] == 100
        assert result["current_partitions"] == 200

    def test_zero_bytes_returns_one_partition(self) -> None:
        result = json.loads(
            execute_tool(
                AgentToolName.CALCULATE_OPTIMAL_PARTITIONS,
                {"total_shuffle_bytes": 0},
                make_agent_context(),
                make_default_static(),
            )
        )
        assert result["optimal_partitions"] == 1


class TestCompareConfigs:
    def test_diffs_configs(self) -> None:
        ctx = make_agent_context(config={"spark.executor.memory": "4g"})
        result = json.loads(
            execute_tool(
                AgentToolName.COMPARE_CONFIGS,
                {"proposed_changes": {"spark.executor.memory": "8g", "spark.executor.cores": "8"}},
                ctx,
                make_default_static(),
            )
        )
        assert result["total_changes"] == 2
        mem_change = next(c for c in result["changes"] if c["parameter"] == "spark.executor.memory")
        assert mem_change["current_value"] == "4g"
        assert mem_change["proposed_value"] == "8g"
        assert mem_change["changed"] is True


class TestUnknownTool:
    def test_raises_on_unknown(self) -> None:
        with pytest.raises(ToolExecutionError, match="Unknown tool"):
            execute_tool("nonexistent", {}, make_agent_context(), make_default_static())
