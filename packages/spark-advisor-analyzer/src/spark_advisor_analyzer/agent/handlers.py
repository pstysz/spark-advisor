import json
import math
from typing import Any

from pydantic import BaseModel, ValidationError

from spark_advisor_analyzer.agent.context import AgentContext
from spark_advisor_analyzer.agent.tools import (
    AgentToolName,
    CalculateOptimalPartitionsInput,
    CompareConfigsInput,
    GetStageDetailsInput,
)
from spark_advisor_analyzer.ai.tool_config import IMPORTANT_KEYS
from spark_advisor_models.util import format_bytes
from spark_advisor_rules import StaticAnalysisService

_TOOL_INPUT_MODELS: dict[AgentToolName, type[BaseModel]] = {
    AgentToolName.GET_STAGE_DETAILS: GetStageDetailsInput,
    AgentToolName.CALCULATE_OPTIMAL_PARTITIONS: CalculateOptimalPartitionsInput,
    AgentToolName.COMPARE_CONFIGS: CompareConfigsInput,
}


class ToolExecutionError(Exception):
    pass


def execute_tool(
    name: str,
    input_data: dict[str, Any],
    context: AgentContext,
    static_analysis: StaticAnalysisService,
) -> str:
    try:
        tool = AgentToolName(name)
    except ValueError as err:
        raise ToolExecutionError(f"Unknown tool: {name}") from err

    if tool == AgentToolName.SUBMIT_FINAL_REPORT:
        raise ToolExecutionError("submit_final_report is handled by the orchestrator, not execute_tool")

    input_model = _TOOL_INPUT_MODELS.get(tool)
    validated: BaseModel | None = None
    if input_model is not None:
        try:
            validated = input_model.model_validate(input_data)
        except ValidationError as e:
            raise ToolExecutionError(f"Invalid input for {name}: {e}") from e

    match tool:
        case AgentToolName.GET_JOB_OVERVIEW:
            return _handle_get_job_overview(context)
        case AgentToolName.GET_STAGE_DETAILS:
            assert isinstance(validated, GetStageDetailsInput)
            return _handle_get_stage_details(context, validated)
        case AgentToolName.RUN_RULES_ENGINE:
            return _handle_run_rules_engine(context, static_analysis)
        case AgentToolName.CALCULATE_OPTIMAL_PARTITIONS:
            assert isinstance(validated, CalculateOptimalPartitionsInput)
            return _handle_calculate_optimal_partitions(context, validated)
        case AgentToolName.COMPARE_CONFIGS:
            assert isinstance(validated, CompareConfigsInput)
            return _handle_compare_configs(context, validated)
        case _:
            raise ToolExecutionError(f"Unhandled tool: {tool}")


def _handle_get_job_overview(context: AgentContext) -> str:
    job = context.job
    total_tasks = sum(s.tasks.task_count for s in job.stages)
    total_shuffle_read = sum(s.total_shuffle_read_bytes for s in job.stages)
    total_shuffle_write = sum(s.total_shuffle_write_bytes for s in job.stages)
    total_spill = sum(s.spill_to_disk_bytes for s in job.stages)
    total_input = sum(s.input_bytes for s in job.stages)

    config_snapshot = {k: job.config.get(k) for k in IMPORTANT_KEYS if job.config.get(k)}

    overview: dict[str, Any] = {
        "app_id": job.app_id,
        "app_name": job.app_name,
        "spark_version": job.spark_version,
        "duration_ms": job.duration_ms,
        "duration_min": round(job.duration_ms / 60_000, 1),
        "stage_count": len(job.stages),
        "total_tasks": total_tasks,
        "total_input": format_bytes(total_input),
        "total_shuffle_read": format_bytes(total_shuffle_read),
        "total_shuffle_write": format_bytes(total_shuffle_write),
        "total_spill_to_disk": format_bytes(total_spill),
        "stages_summary": [
            {
                "stage_id": s.stage_id,
                "name": s.stage_name,
                "tasks": s.tasks.task_count,
                "gc_percent": round(s.gc_time_percent, 1) if s.sum_executor_run_time_ms > 0 else 0.0,
                "has_spill": s.spill_to_disk_bytes > 0,
                "has_shuffle": s.total_shuffle_read_bytes > 0 or s.total_shuffle_write_bytes > 0,
                "skew_ratio": round(s.tasks.duration_skew_ratio, 1),
                "failed_tasks": s.failed_task_count,
            }
            for s in job.stages
        ],
        "config": config_snapshot,
    }

    if job.executors:
        overview["executors"] = {
            "count": job.executors.executor_count,
            "total_cores": job.executors.total_cores,
            "memory_utilization_percent": round(job.executors.memory_utilization_percent, 1),
            "failed_tasks": job.executors.failed_tasks,
        }

    return json.dumps(overview)


def _handle_get_stage_details(context: AgentContext, input_data: GetStageDetailsInput) -> str:
    stage = next((s for s in context.job.stages if s.stage_id == input_data.stage_id), None)
    if stage is None:
        available = [s.stage_id for s in context.job.stages]
        raise ToolExecutionError(f"Stage {input_data.stage_id} not found. Available stages: {available}")

    return stage.model_dump_json()


def _handle_run_rules_engine(context: AgentContext, static_analysis: StaticAnalysisService) -> str:
    results = context.get_or_run_rules(static_analysis)

    findings = [
        {
            "rule_id": r.rule_id,
            "severity": r.severity.value,
            "title": r.title,
            "message": r.message,
            "stage_id": r.stage_id,
            "current_value": r.current_value,
            "recommended_value": r.recommended_value,
        }
        for r in results
    ]
    return json.dumps({"findings_count": len(findings), "findings": findings})


def _handle_calculate_optimal_partitions(
    context: AgentContext,
    input_data: CalculateOptimalPartitionsInput,
) -> str:
    total_bytes = input_data.total_shuffle_bytes
    target_mb = input_data.target_partition_mb
    target_bytes = target_mb * 1024 * 1024

    optimal = max(1, math.ceil(total_bytes / target_bytes)) if total_bytes > 0 else 1
    current = context.job.config.shuffle_partitions

    actual_size_mb = round(total_bytes / (optimal * 1024 * 1024), 1) if optimal > 0 else 0

    return json.dumps(
        {
            "current_partitions": current,
            "optimal_partitions": optimal,
            "total_shuffle_bytes": total_bytes,
            "total_shuffle_formatted": format_bytes(total_bytes),
            "target_partition_size_mb": target_mb,
            "actual_partition_size_mb": actual_size_mb,
        }
    )


def _handle_compare_configs(context: AgentContext, input_data: CompareConfigsInput) -> str:
    changes = []
    for key, new_value in input_data.proposed_changes.items():
        current = context.job.config.get(key)
        changes.append(
            {
                "parameter": key,
                "current_value": current or "(not set)",
                "proposed_value": new_value,
                "changed": current != new_value,
            }
        )
    return json.dumps({"changes": changes, "total_changes": len(changes)})
