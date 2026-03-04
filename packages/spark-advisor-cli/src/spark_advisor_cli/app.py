import typer

from spark_advisor_cli.commands.analyze import analyze
from spark_advisor_cli.commands.scan import scan
from spark_advisor_cli.commands.version import version

app = typer.Typer(
    name="spark-advisor",
    help="AI-powered Apache Spark job analyzer and configuration advisor",
    no_args_is_help=True,
)

app.command()(analyze)
app.command()(scan)
app.command()(version)


def main() -> None:
    app()
