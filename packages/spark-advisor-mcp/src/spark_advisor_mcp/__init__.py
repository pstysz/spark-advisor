"""MCP server exposing spark-advisor tools for Claude Desktop and other MCP clients."""

from spark_advisor_mcp.formatting import (
    format_analysis_result,
    format_config_table,
    format_job_comparison,
    format_job_overview,
    format_rule_results,
    format_scan_results,
    format_stage_details,
    format_suggested_config,
)
from spark_advisor_mcp.metric_explanations import METRIC_DESCRIPTIONS, format_metric_explanation

__all__ = [
    "METRIC_DESCRIPTIONS",
    "format_analysis_result",
    "format_config_table",
    "format_job_comparison",
    "format_job_overview",
    "format_metric_explanation",
    "format_rule_results",
    "format_scan_results",
    "format_stage_details",
    "format_suggested_config",
]
