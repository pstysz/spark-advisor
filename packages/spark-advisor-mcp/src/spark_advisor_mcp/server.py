from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from mcp.server.fastmcp import FastMCP

from spark_advisor_mcp.formatting import (
    format_analysis_result,
    format_config_table,
    format_job_comparison,
    format_job_overview,
    format_scan_results,
    format_stage_details,
    format_suggested_config,
)
from spark_advisor_mcp.metric_explanations import format_metric_explanation
from spark_advisor_models.model import AnalysisMode

if TYPE_CHECKING:
    from spark_advisor_models.model import JobAnalysis

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("spark-advisor")


def _format_error(title: str, detail: str) -> str:
    return f"## Error\n\n**{title}**\n\n{detail}"


def _validate_history_server_url(url: str | None) -> None:
    if url is not None and not url.startswith(("http://", "https://")):
        raise ValueError(f"Invalid History Server URL: `{url}`. Must start with http:// or https://.")


def _load_job(source: str, history_server: str | None = None) -> JobAnalysis:
    _validate_history_server_url(history_server)

    if history_server:
        from spark_advisor_hs_connector.history_server.client import HistoryServerClient
        from spark_advisor_hs_connector.job_analysis_builder import fetch_job_analysis

        with HistoryServerClient(history_server) as client:
            return fetch_job_analysis(client, source)

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: `{source}`")

    from spark_advisor_parser import parse_event_log

    return parse_event_log(path)


def _ai_available() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


@mcp.tool()
def analyze_spark_job(
        source: str,
        history_server: str | None = None,
        mode: AnalysisMode = AnalysisMode.AI,
) -> str:
    """Analyze a Spark job and get optimization recommendations.

    Runs the rules engine (11 deterministic rules detecting data skew, GC pressure,
    disk spill, wrong partition count, etc.) and optionally AI-powered analysis
    with Claude for prioritized recommendations with causal chains.

    Output includes:
    - Job overview with key metrics (duration, stages, executors, shuffle, spill)
    - Quick stats summary
    - Rule findings with severity, current/recommended values, and impact
    - (AI/Agent mode) Prioritized recommendations, causal chain, suggested spark-defaults.conf

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL (e.g. http://yarn:18080). Required when source is an app ID.
        mode: Analysis mode — "static" (rules only), "ai" (rules + Claude AI), "agent" (multi-turn AI with tool use).
    """
    try:
        from spark_advisor_analyzer.factory import create_analysis_context
        from spark_advisor_models.defaults import DEFAULT_THRESHOLDS

        if mode in (AnalysisMode.AI, AnalysisMode.AGENT) and not _ai_available():
            mode = AnalysisMode.STATIC

        job = _load_job(source, history_server)
        use_ai = mode in (AnalysisMode.AI, AnalysisMode.AGENT)

        with create_analysis_context(mode=mode, thresholds=DEFAULT_THRESHOLDS) as orchestrator:
            result = orchestrator.run(job, mode=mode)

        return format_analysis_result(result, use_ai=use_ai)
    except FileNotFoundError as e:
        return _format_error("File not found", str(e))
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("analyze_spark_job failed")
        return _format_error("Analysis failed", f"An unexpected error occurred while analyzing `{source}`.")


@mcp.tool()
def scan_recent_jobs(
        history_server: str,
        limit: int = 20,
) -> str:
    """List recent Spark applications from a History Server.

    Returns a markdown table with app ID, name, duration, status, and Spark version
    for each application.

    Args:
        history_server: Spark History Server URL (e.g. http://yarn:18080).
        limit: Maximum number of applications to list (1-1000). Defaults to 20.
    """
    try:
        _validate_history_server_url(history_server)
        if limit < 1:
            return _format_error("Invalid parameter", "`limit` must be a positive integer.")

        from spark_advisor_hs_connector.history_server.client import HistoryServerClient

        with HistoryServerClient(history_server) as client:
            apps = client.list_applications(limit=limit)
        return format_scan_results(apps)
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("scan_recent_jobs failed")
        return _format_error(
            "Scan failed", f"Could not connect to History Server at `{history_server}`."
        )


@mcp.tool()
def get_job_config(
        source: str,
        history_server: str | None = None,
) -> str:
    """Get the full Spark configuration of a job.

    Returns all spark.* properties grouped by category (executor, shuffle, SQL, dynamic allocation).
    Useful for reviewing current settings before making optimization recommendations.

    Output includes:
    - Job overview (app ID, duration, stages, executors)
    - Spark configuration table grouped by category

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL. Required when source is an app ID.
    """
    try:
        job = _load_job(source, history_server)
        return format_job_overview(job) + "\n\n" + format_config_table(job.config)
    except FileNotFoundError as e:
        return _format_error("File not found", str(e))
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("get_job_config failed")
        return _format_error("Failed to load job", f"Could not load job from `{source}`.")


@mcp.tool()
def suggest_config(
        source: str,
        history_server: str | None = None,
) -> str:
    """Run the rules engine and suggest configuration changes for a Spark job.

    Returns concrete parameter recommendations based on detected issues
    (data skew, GC pressure, spill, wrong partition count, etc.).

    Output includes a table with rule ID, current value, and recommended value
    for each configuration that should be changed.

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL. Required when source is an app ID.
    """
    try:
        from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
        from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

        job = _load_job(source, history_server)
        thresholds = DEFAULT_THRESHOLDS
        static = StaticAnalysisService(rules_for_threshold(thresholds))
        rule_results = static.analyze(job)
        return format_suggested_config(rule_results)
    except FileNotFoundError as e:
        return _format_error("File not found", str(e))
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("suggest_config failed")
        return _format_error("Suggestion failed", f"Could not analyze `{source}`.")


@mcp.tool()
def explain_metric(
        metric_name: str,
        value: float = 0.0,
) -> str:
    """Explain what a Spark metric means and assess whether its value is healthy.

    Returns the metric definition, the formatted value, and a contextual assessment
    (Healthy / Warning / Critical) based on known thresholds.

    Known metrics: gc_time_percent, spill_to_disk_bytes, data_skew_ratio,
    shuffle_read_bytes, shuffle_write_bytes, executor_count,
    memory_utilization_percent, task_count, input_bytes, duration_ms,
    slot_utilization_percent.

    Args:
        metric_name: Name of the Spark metric to explain.
        value: The metric value to contextualize the explanation.
    """
    try:
        from spark_advisor_models.defaults import DEFAULT_THRESHOLDS

        return format_metric_explanation(metric_name, value, DEFAULT_THRESHOLDS)
    except Exception:
        logger.exception("explain_metric failed")
        return _format_error("Explanation failed", f"Could not explain metric `{metric_name}`.")


@mcp.tool()
def get_stage_details(
        source: str,
        stage_id: int,
        history_server: str | None = None,
) -> str:
    """Get detailed metrics for a specific stage of a Spark job.

    Returns per-stage breakdown including task count, duration quantiles (min/p25/median/p75/max),
    GC time, shuffle read/write, spill, input/output bytes, skew ratio, and failed tasks.

    Use this tool to drill down into a specific stage after running analyze_spark_job.

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        stage_id: The stage ID to inspect (from the job overview or rule findings).
        history_server: Spark History Server URL. Required when source is an app ID.
    """
    try:
        job = _load_job(source, history_server)
        stage = next((s for s in job.stages if s.stage_id == stage_id), None)
        if stage is None:
            available = ", ".join(str(s.stage_id) for s in job.stages)
            return _format_error(
                "Stage not found",
                f"Stage `{stage_id}` not found in job `{job.app_id}`. Available stages: {available}.",
            )
        return format_stage_details(stage)
    except FileNotFoundError as e:
        return _format_error("File not found", str(e))
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("get_stage_details failed")
        return _format_error("Failed to load stage", f"Could not load stage `{stage_id}` from `{source}`.")


@mcp.tool()
def compare_jobs(
        source_a: str,
        source_b: str,
        history_server: str | None = None,
) -> str:
    """Compare two Spark jobs side by side (e.g. before vs after optimization).

    Returns a comparison table with key metrics: duration, total tasks, shuffle volume,
    spill to disk, GC time, executor count, and memory utilization — with delta and
    percentage change for each metric.

    Args:
        source_a: Path to event log or app ID for the first job (baseline).
        source_b: Path to event log or app ID for the second job (optimized).
        history_server: Spark History Server URL. Required when sources are app IDs.
    """
    try:
        job_a = _load_job(source_a, history_server)
        job_b = _load_job(source_b, history_server)
        return format_job_comparison(job_a, job_b)
    except FileNotFoundError as e:
        return _format_error("File not found", str(e))
    except ValueError as e:
        return _format_error("Invalid parameter", str(e))
    except Exception:
        logger.exception("compare_jobs failed")
        return _format_error("Comparison failed", "Could not compare the two jobs.")


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
