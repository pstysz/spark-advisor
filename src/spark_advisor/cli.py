from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor.analysis.config import DEFAULT_MODEL
from spark_advisor.output.console import (
    print_analysis_result,
    print_job_overview,
    print_scan_results,
)
from spark_advisor.services.job_service import JobService

app = typer.Typer(
    name="spark-advisor",
    help="AI-powered Apache Spark job analyzer and configuration advisor",
    no_args_is_help=True,
)
console = Console()


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
    job_service = JobService(history_server)

    with console.status("[bold blue]Loading job data...[/]"):
        try:
            job = job_service.load_job(source)
        except FileNotFoundError as e:
            console.print(f"[red]Error: {e}[/]")
            raise typer.Exit(code=1) from e

    print_job_overview(job)

    with console.status("[bold blue]Running analysis...[/]"):
        result = job_service.analyze(job, ai_enabled, model)

    print_analysis_result(result, use_ai=ai_enabled, output_config=output_config)


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
    apps = JobService(history_server).list_applications(limit=limit)
    print_scan_results(apps, limit=limit)


@app.command()
def version() -> None:
    """Show version information."""
    console.print("spark-advisor [bold]v0.1.0[/]")


if __name__ == "__main__":
    app()
