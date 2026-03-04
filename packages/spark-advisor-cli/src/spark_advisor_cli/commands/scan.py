from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

console = Console()


def scan(
    history_server: Annotated[
        str,
        typer.Option("--history-server", "-hs", help="Spark History Server URL (e.g. http://yarn:18080)"),
    ],
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Maximum number of applications to list"),
    ] = 20,
) -> None:
    """List recent Spark applications from History Server."""
    from spark_advisor_hs_connector.history_server_client import HistoryServerClient

    with console.status("[bold blue]Fetching applications...[/]"):
        try:
            with HistoryServerClient(history_server) as client:
                apps = client.list_applications(limit=limit)
        except Exception as e:
            console.print(f"[red]Error connecting to History Server: {e}[/]")
            raise typer.Exit(code=1) from e

    if not apps:
        console.print("[yellow]No applications found.[/]")
        return

    table = Table(title=f"Recent Spark Applications ({len(apps)})")
    table.add_column("App ID", style="bold")
    table.add_column("Name")
    table.add_column("Duration", justify="right")
    table.add_column("Status")
    table.add_column("Spark Version")

    for app in apps:
        latest = app.attempts[-1] if app.attempts else None
        duration = "-"
        status = ""
        spark_version = ""

        if latest:
            if latest.duration > 0:
                duration_min = latest.duration / 60_000
                duration = f"{duration_min:.1f} min"
            status = "[green]completed[/]" if latest.completed else "[yellow]running[/]"
            spark_version = latest.appSparkVersion

        table.add_row(app.id, app.name, duration, status, spark_version)

    console.print(table)
