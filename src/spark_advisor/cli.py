from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor import SparkAdvisor
from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.analysis.rules import rules_for_threshold
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor.api.anthropic_client import AnthropicClient
from spark_advisor.api.history_server_client import HistoryServerClient
from spark_advisor.config import DEFAULT_MODEL, Thresholds
from spark_advisor.model import AnalysisResult
from spark_advisor.model.metrics import JobAnalysis
from spark_advisor.util.console import (
    print_analysis_result,
    print_job_overview,
    print_scan_results,
)
from spark_advisor.util.event_parser import parse_event_log

app = typer.Typer(
    name="spark-advisor",
    help="AI-powered Apache Spark job analyzer and configuration advisor",
    no_args_is_help=True,
)
console = Console()


def _load_job(source: str, history_server: str | None) -> JobAnalysis:
    if history_server:
        with HistoryServerClient(history_server) as client:
            return client.fetch(source)
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: {source}")
    return parse_event_log(path)


def _run_analysis(
        job: JobAnalysis, *, ai_enabled: bool, model: str, thresholds: Thresholds | None = None
) -> AnalysisResult:
    t = thresholds or Thresholds()
    static = StaticAnalysisService(rules_for_threshold(t))
    if not ai_enabled:
        return SparkAdvisor(static).run(job)
    with AnthropicClient() as client:
        return SparkAdvisor(static, LlmAnalysisService(client)).run(job, model=model)


@app.command()
def analyze(
        source: Annotated[
            str,
            typer.Argument(help="Path to event log file OR Spark application ID"),
        ],
        history_server: Annotated[
            str | None,
            typer.Option("--history-server", "-s", help="Spark History Server URL"),
        ] = None,
        ai_enabled: Annotated[
            bool,
            typer.Option(
                "--ai/--no-ai",
                help="Enable AI-powered analysis (requires ANTHROPIC_API_KEY)",
            ),
        ] = True,
        model: Annotated[
            str,
            typer.Option("--model", "-m", help="Claude model to use"),
        ] = DEFAULT_MODEL,
        output_config: Annotated[
            Path | None,
            typer.Option("--output-config", "-o", help="Write suggested config to file"),
        ] = None,
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = _load_job(source, history_server)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e

    print_job_overview(console, job)

    t = Thresholds()  # ToDo: add option to pass thresholds json as a parameter
    with console.status("[bold blue]Running analysis...[/]"):
        result = _run_analysis(job, ai_enabled=ai_enabled, model=model, thresholds=t)

    print_analysis_result(console, result, use_ai=ai_enabled, output_config=output_config)


@app.command()
def scan(
        history_server: Annotated[
            str,
            typer.Option("--history-server", "-s", help="Spark History Server URL"),
        ],
        limit: Annotated[
            int,
            typer.Option("--limit", "-n", help="Number of recent applications to scan"),
        ] = 10,
) -> None:
    """Scan recent Spark applications and flag potential issues."""
    with HistoryServerClient(history_server) as client:
        apps = client.list_applications(limit=limit)
    print_scan_results(console, apps, limit=limit)


@app.command()
def version() -> None:
    """Show version information."""
    console.print("spark-advisor [bold]v0.1.0[/]")


if __name__ == "__main__":
    app()
