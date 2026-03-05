import logging
import os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from spark_advisor_mcp.formatting import (
    format_analysis_result,
    format_config_table,
    format_job_overview,
    format_metric_explanation,
    format_scan_results,
    format_suggested_config,
)
from spark_advisor_models.model import JobAnalysis

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("spark-advisor")


def _load_job(source: str, history_server: str | None = None) -> JobAnalysis:
    if history_server:
        from spark_advisor_hs_connector.history_server_client import HistoryServerClient
        from spark_advisor_hs_connector.hs_fetcher import fetch_job_analysis

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
    no_ai: bool = False,
    agent: bool = False,
) -> str:
    """Analyze a Spark job and get optimization recommendations.

    Runs the rules engine (11 deterministic rules detecting data skew, GC pressure,
    disk spill, wrong partition count, etc.) and optionally AI-powered analysis
    with Claude for prioritized recommendations with causal chains.

    Args:
        source: Path to event log file (.json or .json.gz) or app ID (with history_server).
        history_server: Spark History Server URL (e.g. http://yarn:18080). Required when source is an app ID.
        no_ai: Disable AI analysis (rules only). Defaults to False.
        agent: Use agent mode (multi-turn AI with tool use for deeper analysis). Defaults to False.
    """
    from spark_advisor_analyzer.factory import create_analysis_stack
    from spark_advisor_models.config import AiSettings, Thresholds
    from spark_advisor_models.model.output import AnalysisMode

    if agent and not _ai_available():
        return "Error: agent mode requires ANTHROPIC_API_KEY environment variable."

    job = _load_job(source, history_server)
    thresholds = Thresholds()
    use_ai = not no_ai and _ai_available()
    mode = AnalysisMode.AGENT if agent else AnalysisMode.STANDARD

    client = None
    ai_settings: AiSettings | None = None
    if use_ai:
        from spark_advisor_analyzer.ai.client import AnthropicClient

        ai_settings = AiSettings()
        client = AnthropicClient(timeout=ai_settings.api_timeout)
        client.open()

    try:
        orchestrator = create_analysis_stack(
            client=client, ai_settings=ai_settings, thresholds=thresholds,
        )
        result = orchestrator.run(job, mode=mode)
    finally:
        if client is not None:
            client.close()

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
    from spark_advisor_hs_connector.history_server_client import HistoryServerClient

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
    from spark_advisor_models.config import Thresholds
    from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

    job = _load_job(source, history_server)
    thresholds = Thresholds()
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
