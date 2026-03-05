import os
import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor_cli.event_log.parser import parse_event_log
from spark_advisor_cli.output.console import print_analysis_result, print_job_overview, print_stage_breakdown
from spark_advisor_models.config import AiSettings, Thresholds
from spark_advisor_models.model import AnalysisResult, JobAnalysis
from spark_advisor_models.model.output import AnalysisMode, OutputFormat

console = Console()


def _load_job(source: str, history_server: str | None) -> JobAnalysis:
    if history_server:
        return _fetch_from_history_server(source, history_server)
    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f"Event log file not found: {source}")
    return parse_event_log(path)


def _fetch_from_history_server(app_id: str, history_server_url: str) -> JobAnalysis:
    from spark_advisor_hs_connector.history_server_client import HistoryServerClient
    from spark_advisor_hs_connector.hs_fetcher import fetch_job_analysis

    with HistoryServerClient(history_server_url) as client:
        return fetch_job_analysis(client, app_id)


def _resolve_ai_enabled(no_ai: bool) -> bool:
    if no_ai:
        return False
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def _run_analysis(
    job: JobAnalysis,
    thresholds: Thresholds,
    *,
    use_ai: bool,
    mode: AnalysisMode,
    model: str,
) -> AnalysisResult:
    from spark_advisor_analyzer.factory import create_analysis_stack

    client = None
    ai_settings: AiSettings | None = None

    if use_ai:
        from spark_advisor_analyzer.ai.client import AnthropicClient

        ai_settings = AiSettings(model=model)
        client = AnthropicClient(timeout=ai_settings.api_timeout)
        client.open()

    try:
        orchestrator = create_analysis_stack(
            client=client,
            ai_settings=ai_settings,
            thresholds=thresholds,
        )
        return orchestrator.run(job, mode=mode)
    finally:
        if client is not None:
            client.close()


def analyze(
    source: Annotated[
        str,
        typer.Argument(help="App ID (with --history-server) or path to event log file (.json or .json.gz)"),
    ],
    history_server: Annotated[
        str | None,
        typer.Option("--history-server", "-hs", help="Spark History Server URL (e.g. http://yarn:18080)"),
    ] = None,
    no_ai: Annotated[
        bool,
        typer.Option("--no-ai", help="Disable AI analysis (rules only)"),
    ] = False,
    agent: Annotated[
        bool,
        typer.Option("--agent", help="Use agent mode (multi-turn AI analysis with tool use)"),
    ] = False,
    model: Annotated[
        str,
        typer.Option("--model", "-m", help="Claude model for AI analysis"),
    ] = "claude-sonnet-4-6",
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
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    if agent and no_ai:
        console.print("[red]Error: --agent requires AI (cannot use with --no-ai)[/]")
        raise typer.Exit(code=1)

    if agent and not os.environ.get("ANTHROPIC_API_KEY"):
        console.print("[red]Error: --agent requires ANTHROPIC_API_KEY environment variable[/]")
        raise typer.Exit(code=1)

    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = _load_job(source, history_server)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e
        except Exception as e:
            console.print(f"[red]Error fetching job data: {e}[/]")
            raise typer.Exit(code=1) from e

    thresholds = Thresholds()
    use_ai = _resolve_ai_enabled(no_ai)
    analysis_mode = AnalysisMode.AGENT if agent else AnalysisMode.STANDARD

    if analysis_mode == AnalysisMode.AGENT:
        status_msg = "[bold blue]Running agent analysis (multi-turn AI)...[/]"
    elif use_ai:
        status_msg = "[bold blue]Running analysis (rules + AI)...[/]"
    else:
        status_msg = "[bold blue]Running analysis...[/]"

    with console.status(status_msg):
        try:
            result = _run_analysis(
                job,
                thresholds,
                use_ai=use_ai,
                mode=analysis_mode,
                model=model,
            )
        except Exception as e:
            console.print(f"[red]Analysis error: {e}[/]")
            raise typer.Exit(code=1) from e

    if output_format == OutputFormat.JSON:
        sys.stdout.write(result.model_dump_json(indent=2) + "\n")
    else:
        print_job_overview(console, job)
        if verbose:
            print_stage_breakdown(console, job)
        print_analysis_result(
            console,
            result,
            use_ai=use_ai or analysis_mode == AnalysisMode.AGENT,
            output_config=output,
        )

    if output and not use_ai:
        console.print("[yellow]Warning: --output requires AI analysis to generate config file.[/]")
