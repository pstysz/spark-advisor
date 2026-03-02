import sys
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor_cli.event_log.parser import parse_event_log
from spark_advisor_cli.output.console import print_analysis_result, print_job_overview, print_stage_breakdown
from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import AnalysisResult, JobAnalysis
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

console = Console()


class OutputFormat(StrEnum):
    TEXT = "text"
    JSON = "json"


def _load_job(source: str) -> JobAnalysis:
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: {source}")
    return parse_event_log(path)


def _run_local_analysis(job: JobAnalysis, thresholds: Thresholds) -> AnalysisResult:
    static = StaticAnalysisService(rules_for_threshold(thresholds))
    rule_results = static.analyze(job)
    return AnalysisResult(app_id=job.app_id, job=job, rule_results=rule_results, ai_report=None)


def analyze(
        source: Annotated[
            str,
            typer.Argument(help="Path to event log file (.json or .json.gz)"),
        ],
        gateway: Annotated[
            str | None,
            typer.Option("--gateway", "-g", help="Gateway URL for remote analysis (e.g. http://localhost:8000)"),
        ] = None,
        output_config: Annotated[
            Path | None,
            typer.Option("--output-config", "-o", help="Write suggested config to file"),
        ] = None,
        format: Annotated[
            OutputFormat,
            typer.Option("--format", "-f", help="Output format: text or json"),
        ] = OutputFormat.TEXT,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Show per-stage breakdown"),
        ] = False,
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    if gateway is not None:
        console.print("[yellow]Remote mode via gateway is not implemented yet.[/]")
        raise typer.Exit(code=1)

    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = _load_job(source)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e

    t = Thresholds()
    with console.status("[bold blue]Running analysis...[/]"):
        result = _run_local_analysis(job, t)

    if format == OutputFormat.JSON:
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    else:
        print_job_overview(console, job)
        if verbose:
            print_stage_breakdown(console, job)
        print_analysis_result(console, result, use_ai=False, output_config=output_config)
