from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor import AdviceOrchestrator
from spark_advisor.ai.llm_analysis_service import LlmAnalysisService
from spark_advisor.analysis.rules import rules_for_threshold
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor.api.anthropic_client import AnthropicClient
from spark_advisor.config import AnalyzerSettings
from spark_advisor.util.console import (
    print_analysis_result,
    print_job_overview,
)
from spark_advisor.util.event_parser import parse_event_log
from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import AnalysisResult, JobAnalysis

app = typer.Typer(
    name="spark-advisor",
    help="AI-powered Apache Spark job analyzer and configuration advisor",
    no_args_is_help=True,
)
console = Console()
_settings = AnalyzerSettings()


def _load_job(source: str) -> JobAnalysis:
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
        return AdviceOrchestrator(static).run(job)
    with AnthropicClient(_settings.ai_settings.api_timeout) as client:
        llm = LlmAnalysisService(client, _settings.ai_settings, t)
        return AdviceOrchestrator(static, llm).run(job)


@app.command()
def analyze(
        source: Annotated[
            str,
            typer.Argument(help="Path to event log file (.json or .json.gz)"),
        ],
        ai_enabled: Annotated[
            bool,
            typer.Option(
                "--ai/--no-ai",
                help="Enable AI-powered analysis (requires ANTHROPIC_API_KEY)",
            ),
        ] = _settings.ai_settings.enabled,
        model: Annotated[
            str,
            typer.Option("--model", "-m", help="Claude model to use"),
        ] = _settings.ai_settings.model,
        output_config: Annotated[
            Path | None,
            typer.Option("--output-config", "-o", help="Write suggested config to file"),
        ] = None,
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = _load_job(source)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e

    print_job_overview(console, job)

    t = Thresholds()  # ToDo: add option to pass thresholds json as a parameter
    with console.status("[bold blue]Running analysis...[/]"):
        result = _run_analysis(job, ai_enabled=ai_enabled, model=model, thresholds=t)

    print_analysis_result(console, result, use_ai=ai_enabled, output_config=output_config)


@app.command()
def version() -> None:
    """Show version information."""
    console.print("spark-advisor [bold]v0.1.0[/]")


if __name__ == "__main__":
    app()
