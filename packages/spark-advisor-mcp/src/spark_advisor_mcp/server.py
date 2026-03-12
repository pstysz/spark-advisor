import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from spark_advisor_mcp.formatting import (
    format_analysis_result,
    format_config_table,
    format_job_overview,
    format_scan_results,
    format_suggested_config,
)
from spark_advisor_mcp.metric_explanations import format_metric_explanation
from spark_advisor_models.model import JobAnalysis

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("spark-advisor")


def _load_job(source: str, history_server: str | None = None) -> JobAnalysis:
    if history_server:
        from spark_advisor_hs_connector.history_server.client import HistoryServerClient
        from spark_advisor_hs_connector.job_analysis_builder import fetch_job_analysis

        with HistoryServerClient(history_server) as client:
            return fetch_job_analysis(client, source)

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: {source}")

    from spark_advisor_cli.event_log.parser import parse_event_log

    return parse_event_log(path)


def _ai_available() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


@mcp.tool()
def analyze_spark_job(
    source: str,
    history_server: str | None = None,
    mode: str = "ai",
) -> str:
    """Analyze a Spark job and get optimization recommendations.

    Runs the rules engine (11 deterministic rules detecting data skew, GC pressure,
    disk spill, wrong partition count, etc.) and optionally AI-powered analysis
    with Claude for prioritized recommendations with causal chains.

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL (e.g. http://yarn:18080). Required when source is an app ID.
        mode: Analysis mode — "static" (rules only), "ai" (rules + AI), "agent" (multi-turn AI).
    """
    from spark_advisor_analyzer.factory import create_analysis_context
    from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
    from spark_advisor_models.model import AnalysisMode

    analysis_mode = AnalysisMode(mode)

    if analysis_mode in (AnalysisMode.AI, AnalysisMode.AGENT) and not _ai_available():
        analysis_mode = AnalysisMode.STATIC

    job = _load_job(source, history_server)
    use_ai = analysis_mode in (AnalysisMode.AI, AnalysisMode.AGENT)

    with create_analysis_context(mode=analysis_mode, thresholds=DEFAULT_THRESHOLDS) as orchestrator:
        result = orchestrator.run(job, mode=analysis_mode)

    return format_analysis_result(result, use_ai=use_ai)


@mcp.tool()
def scan_recent_jobs(
    history_server: str,
    limit: int = 20,
) -> str:
    """List recent Spark applications from a History Server.

    Args:
        history_server: Spark History Server URL (e.g. http://yarn:18080).
        limit: Maximum number of applications to list. Defaults to 20.
    """
    from spark_advisor_hs_connector.history_server.client import HistoryServerClient

    with HistoryServerClient(history_server) as client:
        apps = client.list_applications(limit=limit)
    return format_scan_results(apps)


@mcp.tool()
def get_job_config(
    source: str,
    history_server: str | None = None,
) -> str:
    """Get the full Spark configuration of a job.

    Returns all spark.* properties as a table. Useful for reviewing current settings
    before making optimization recommendations.

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL. Required when source is an app ID.
    """
    job = _load_job(source, history_server)
    return format_job_overview(job) + "\n\n" + format_config_table(job.config)


@mcp.tool()
def suggest_config(
    source: str,
    history_server: str | None = None,
) -> str:
    """Run the rules engine and suggest configuration changes for a Spark job.

    Returns concrete parameter recommendations based on detected issues
    (data skew, GC pressure, spill, wrong partition count, etc.).

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL. Required when source is an app ID.
    """
    from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
    from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

    job = _load_job(source, history_server)
    thresholds = DEFAULT_THRESHOLDS
    static = StaticAnalysisService(rules_for_threshold(thresholds))
    rule_results = static.analyze(job)
    return format_suggested_config(rule_results)


@mcp.tool()
def explain_metric(
    metric_name: str,
    value: float = 0.0,
) -> str:
    """Explain what a Spark metric means and how to interpret its value.

    Known metrics: gc_time_percent, spill_to_disk_bytes, data_skew_ratio,
    shuffle_read_bytes, shuffle_write_bytes, executor_count,
    memory_utilization_percent, task_count, input_bytes, duration_ms.

    Args:
        metric_name: Name of the Spark metric to explain.
        value: The metric value to contextualize the explanation.
    """
    return format_metric_explanation(metric_name, value)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
