import os
import sys
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError
from rich.console import Console

from spark_advisor_cli.event_log.parser import parse_event_log
from spark_advisor_cli.output.console import print_analysis_result
from spark_advisor_models.config import Thresholds
from spark_advisor_models.defaults import DEFAULT_MODEL, DEFAULT_THRESHOLDS
from spark_advisor_models.model import AnalysisMode, AnalysisResult, JobAnalysis, OutputFormat

console = Console()


def _parse_thresholds(value: str | None) -> Thresholds:
    if value is None:
        return DEFAULT_THRESHOLDS
    try:
        return Thresholds.model_validate_json(value)
    except ValidationError as e:
        raise typer.BadParameter(f"Invalid thresholds JSON: {e}") from e


def _load_job(source: str, history_server: str | None) -> JobAnalysis:
    if history_server:
        return _fetch_from_history_server(source, history_server)
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: {source}")
    return parse_event_log(path)


def _fetch_from_history_server(app_id: str, history_server_url: str) -> JobAnalysis:
    from spark_advisor_hs_connector.history_server.client import HistoryServerClient
    from spark_advisor_hs_connector.job_analysis_builder import fetch_job_analysis

    with HistoryServerClient(history_server_url) as client:
        return fetch_job_analysis(client, app_id)


def _validate_mode(mode: AnalysisMode) -> AnalysisMode:
    if not os.environ.get("ANTHROPIC_API_KEY") and mode != AnalysisMode.STATIC:
        if mode == AnalysisMode.AGENT:
            console.print("[red]Error: --mode agent/ai requires ANTHROPIC_API_KEY environment variable[/]")
            raise typer.Exit(code=1)
        return AnalysisMode.STATIC
    return mode


def _run_analysis(
        job: JobAnalysis,
        thresholds: Thresholds,
        *,
        mode: AnalysisMode,
        model: str,
) -> AnalysisResult:
    from spark_advisor_analyzer.factory import create_analysis_context

    with create_analysis_context(mode=mode, model=model, thresholds=thresholds) as orchestrator:
        return orchestrator.run(job, mode=mode)


def analyze(
        source: Annotated[
            str,
            typer.Argument(help="App ID (with --history-server) or path to event log file (.json or .json.gz)"),
        ],
        history_server: Annotated[
            str | None,
            typer.Option("--history-server", "-hs", help="Spark History Server URL (e.g. http://yarn:18080)"),
        ] = None,
        mode: Annotated[
            AnalysisMode,
            typer.Option("--mode", help="Analysis mode: static (rules only), ai (rules + AI), agent (multi-turn AI)"),
        ] = AnalysisMode.AI,
        model: Annotated[
            str,
            typer.Option("--model", "-m", help="Claude model for AI analysis"),
        ] = DEFAULT_MODEL,
        output: Annotated[
            Path | None,
            typer.Option("--output", "-o", help="Write suggested config to file (default console)"),
        ] = None,
        output_format: Annotated[
            OutputFormat,
            typer.Option("--format", "-f", help="Output format: text or json"),
        ] = OutputFormat.TEXT,
        verbose: Annotated[
            bool,
            typer.Option("--verbose", "-v", help="Show per-stage breakdown"),
        ] = False,
        thresholds_json: Annotated[
            str | None,
            typer.Option(
                "--thresholds",
                help="Rule thresholds as JSON (e.g. '{\"skew_warning_ratio\": 3.0}'). Unset fields use defaults.",
            ),
        ] = None,
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    mode = _validate_mode(mode)

    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = _load_job(source, history_server)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e
        except Exception as e:
            console.print(f"[red]Error fetching job data: {e}[/]")
            raise typer.Exit(code=1) from e

    thresholds = _parse_thresholds(thresholds_json)

    if mode == AnalysisMode.AGENT:
        status_msg = "[bold blue]Running agent analysis (multi-turn AI)...[/]"
    elif mode == AnalysisMode.AI:
        status_msg = "[bold blue]Running analysis (rules + AI)...[/]"
    else:
        status_msg = "[bold blue]Running analysis...[/]"

    with console.status(status_msg):
        try:
            result = _run_analysis(
                job,
                thresholds,
                mode=mode,
                model=model,
            )
        except Exception as e:
            console.print(f"[red]Analysis error: {e}[/]")
            raise typer.Exit(code=1) from e

    if output_format == OutputFormat.JSON:
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    else:
        print_analysis_result(
            console,
            result,
            mode=mode,
            verbose=verbose,
            output_config=output,
            thresholds=thresholds,
        )
