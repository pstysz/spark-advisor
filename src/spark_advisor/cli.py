"""CLI entry point for spark-advisor.

Typer is a modern CLI framework that uses type hints to generate
argument parsing, help text, and shell completion automatically.
Think of it as Spring Boot's @RestController but for CLI.
"""

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from spark_advisor.analysis.rules import run_rules
from spark_advisor.output.console import (
    print_ai_report,
    print_job_overview,
    print_rule_results,
)

app = typer.Typer(
    name="spark-advisor",
    help="🔥 AI-powered Apache Spark job analyzer and configuration advisor",
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
    ai: Annotated[
        bool,
        typer.Option(
            "--ai/--no-ai",
            help="Enable AI-powered analysis (requires ANTHROPIC_API_KEY)",
        ),
    ] = True,
    model: Annotated[
        str,
        typer.Option("--model", "-m", help="Claude model to use"),
    ] = "claude-sonnet-4-20250514",
    output_config: Annotated[
        Path | None,
        typer.Option("--output-config", "-o", help="Write suggested config to file"),
    ] = None,
) -> None:
    """Analyze a Spark job and get optimization recommendations."""
    with console.status("[bold blue]Loading job data...[/]"):
        job = _load_job(source, history_server)

    print_job_overview(job)

    with console.status("[bold blue]Running analysis rules...[/]"):
        rule_results = run_rules(job)

    print_rule_results(rule_results)

    if ai and rule_results:
        _run_ai_analysis(job, rule_results, model, output_config)
    elif ai and not rule_results:
        console.print("[green]No issues found — skipping AI analysis.[/]")

    if not ai:
        console.print("[dim]AI analysis disabled. Use --ai to enable.[/]")


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
    from spark_advisor.sources.history_server import HistoryServerSource

    with HistoryServerSource(history_server) as source:
        apps = source.list_applications(limit=limit)

    if not apps:
        console.print("[yellow]No applications found.[/]")
        return

    from rich.table import Table

    table = Table(title=f"Recent Spark Applications (last {limit})")
    table.add_column("App ID", min_width=30)
    table.add_column("Name")
    table.add_column("Duration")
    table.add_column("Started")

    for app_info in apps:
        duration_ms = int(app_info.get("duration_ms", "0"))
        duration_str = f"{duration_ms / 60000:.1f} min" if duration_ms > 0 else "—"

        table.add_row(
            app_info["app_id"],
            app_info.get("name", ""),
            duration_str,
            app_info.get("start", ""),
        )

    console.print(table)
    console.print(
        "\n[dim]Use 'spark-advisor analyze <app-id> -s <url>'"
        " to analyze a specific job.[/]"
    )


@app.command()
def version() -> None:
    """Show version information."""
    console.print("spark-advisor [bold]v0.1.0[/]")


def _load_job(source: str, history_server: str | None):
    """Load job data from either History Server or local event log."""
    if history_server:
        from spark_advisor.sources.history_server import HistoryServerSource

        with HistoryServerSource(history_server) as hs:
            return hs.fetch(source)

    path = Path(source)
    if not path.exists():
        console.print(f"[red]Error: File not found: {source}[/]")
        raise typer.Exit(code=1)

    from spark_advisor.sources.event_log import EventLogParser

    return EventLogParser().parse(path)


def _run_ai_analysis(job, rule_results, model: str, output_config: Path | None) -> None:
    """Run AI analysis and print results."""
    import os

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        console.print(
            "[yellow]⚠ Set ANTHROPIC_API_KEY environment variable to enable AI analysis.[/]"
        )
        console.print("[dim]export ANTHROPIC_API_KEY=sk-ant-...[/]")
        return

    from spark_advisor.ai.advisor import AiAdvisor

    with console.status("[bold magenta]🤖 Running AI analysis...[/]"):
        advisor = AiAdvisor(api_key=api_key, model=model)
        report = advisor.analyze(job, rule_results)

    print_ai_report(report)

    if output_config and report.suggested_config:
        output_config.write_text(
            "\n".join(f"{k} = {v}" for k, v in report.suggested_config.items()) + "\n"
        )
        console.print(f"[green]Config written to {output_config}[/]")


if __name__ == "__main__":
    app()
