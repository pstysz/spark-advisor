from importlib.metadata import version as pkg_version

from rich.console import Console

console = Console()


def version() -> None:
    """Show version information."""
    ver = pkg_version("spark-advisor-cli")
    console.print(f"spark-advisor [bold]v{ver}[/]")
