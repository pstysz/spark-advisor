from rich.console import Console

console = Console()


def version() -> None:
    """Show version information."""
    console.print("spark-advisor [bold]v0.1.0[/]")
