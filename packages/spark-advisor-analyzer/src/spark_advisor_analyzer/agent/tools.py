from enum import StrEnum

from anthropic.types import ToolParam
from pydantic import BaseModel, ConfigDict, Field

from spark_advisor_models.model import AnalysisToolInput


class AgentToolName(StrEnum):
    GET_JOB_OVERVIEW = "get_job_overview"
    GET_STAGE_DETAILS = "get_stage_details"
    RUN_RULES_ENGINE = "run_rules_engine"
    CALCULATE_OPTIMAL_PARTITIONS = "calculate_optimal_partitions"
    COMPARE_CONFIGS = "compare_configs"
    SUBMIT_FINAL_REPORT = "submit_final_report"


class GetJobOverviewInput(BaseModel):
    """No input required — returns overview of the job being analyzed."""

    model_config = ConfigDict(frozen=True)


class GetStageDetailsInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_id: int = Field(description="Stage ID to retrieve detailed metrics for.")


class RunRulesEngineInput(BaseModel):
    """No input required — runs all 11 deterministic rules against the job."""

    model_config = ConfigDict(frozen=True)


class CalculateOptimalPartitionsInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_shuffle_bytes: int = Field(description="Total shuffle bytes to partition.")
    target_partition_mb: int = Field(
        default=128,
        description="Target partition size in MB. Default 128MB.",
    )


class CompareConfigsInput(BaseModel):
    model_config = ConfigDict(frozen=True)

    proposed_changes: dict[str, str] = Field(
        description="Map of spark.* config keys to proposed new values.",
    )


AGENT_TOOLS: list[ToolParam] = [
    {
        "name": AgentToolName.GET_JOB_OVERVIEW,
        "description": (
            "Get a summary of the Spark job: app ID, duration, stage count, "
            "executor count, total tasks, shuffle volumes, and key configuration values. "
            "Also includes per-stage flags (skew, spill, GC) to identify problematic stages."
        ),
        "input_schema": GetJobOverviewInput.model_json_schema(),
    },
    {
        "name": AgentToolName.GET_STAGE_DETAILS,
        "description": (
            "Get full metrics for a specific stage: task count, duration distribution "
            "(min/p25/median/p75/max), shuffle read/write, spill, GC time, input/output bytes, "
            "and complete task quantile distributions."
        ),
        "input_schema": GetStageDetailsInput.model_json_schema(),
    },
    {
        "name": AgentToolName.RUN_RULES_ENGINE,
        "description": (
            "Run the deterministic rules engine (11 rules: data skew, spill to disk, "
            "GC pressure, shuffle partitions, executor idle, task failures, small files, "
            "broadcast join, serializer, dynamic allocation, memory overhead). "
            "Returns list of findings with severity and recommendations."
        ),
        "input_schema": RunRulesEngineInput.model_json_schema(),
    },
    {
        "name": AgentToolName.CALCULATE_OPTIMAL_PARTITIONS,
        "description": (
            "Calculate the optimal spark.sql.shuffle.partitions value given total shuffle "
            "bytes and a target partition size. Returns current vs optimal count with "
            "actual partition size."
        ),
        "input_schema": CalculateOptimalPartitionsInput.model_json_schema(),
    },
    {
        "name": AgentToolName.COMPARE_CONFIGS,
        "description": (
            "Compare proposed Spark config changes against current values. "
            "Shows a diff of current vs proposed for each parameter."
        ),
        "input_schema": CompareConfigsInput.model_json_schema(),
    },
    {
        "name": AgentToolName.SUBMIT_FINAL_REPORT,
        "description": (
            "Submit the final analysis report. This terminates the agent loop. "
            "Must include summary, severity, prioritized recommendations (max 7), "
            "and causal chain if problems are related."
        ),
        "input_schema": AnalysisToolInput.model_json_schema(),
    },
]
