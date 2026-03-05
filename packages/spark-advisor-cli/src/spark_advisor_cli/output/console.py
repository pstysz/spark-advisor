from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from spark_advisor_models.model import (
    AdvisorReport,
    AnalysisResult,
    JobAnalysis,
    RuleResult,
    Severity,
)
from spark_advisor_models.util.bytes import format_bytes

SEVERITY_ICONS = {
    Severity.CRITICAL: "[bold red]🔴 CRITICAL[/]",
    Severity.WARNING: "[bold yellow]🟡 WARNING[/]",
    Severity.INFO: "[bold blue]🔵 INFO[/]",
}


def print_job_overview(console: Console, job: JobAnalysis) -> None:
    duration_min = job.duration_ms / 60_000
    stage_count = len(job.stages)
    total_tasks = sum(s.tasks.task_count for s in job.stages)

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column(style="bold")
    table.add_column()
    table.add_row("App ID", job.app_id)
    if job.app_name:
        table.add_row("App Name", job.app_name)
    table.add_row("Duration", f"{duration_min:.1f} min ({job.duration_ms / 1000:.0f}s)")
    table.add_row("Stages", str(stage_count))
    table.add_row("Total Tasks", str(total_tasks))
    if job.config.shuffle_partitions:
        table.add_row("Shuffle Partitions", str(job.config.shuffle_partitions))
    if job.executors:
        table.add_row("Executors", str(job.executors.executor_count))

    console.print(Panel(table, title="[bold]Spark Job Analysis[/]", border_style="blue"))


def print_stage_breakdown(console: Console, job: JobAnalysis) -> None:
    table = Table(title="Stage Breakdown", show_lines=True)
    table.add_column("Stage", style="bold", width=6)
    table.add_column("Name", min_width=15)
    table.add_column("Tasks", justify="right")
    table.add_column("Duration (ms)\nmin / med / max", justify="right")
    table.add_column("Shuffle R / W", justify="right")
    table.add_column("Spill", justify="right")
    table.add_column("GC %", justify="right")
    table.add_column("Input", justify="right")

    for s in job.stages:
        rt = s.tasks.distributions.executor_run_time
        duration_str = f"{rt.min:.0f} / {rt.median:.0f} / {rt.max:.0f}"

        shuffle_str = f"R: {format_bytes(s.total_shuffle_read_bytes)}\nW: {format_bytes(s.total_shuffle_write_bytes)}"
        spill_str = format_bytes(s.spill_to_disk_bytes) if s.spill_to_disk_bytes > 0 else "-"
        input_str = format_bytes(s.input_bytes) if s.input_bytes > 0 else "-"

        gc_pct = s.gc_time_percent
        gc_str = f"{gc_pct:.0f}%"
        if gc_pct > 40:
            gc_str = f"[bold red]{gc_pct:.0f}%[/]"
        elif gc_pct > 20:
            gc_str = f"[yellow]{gc_pct:.0f}%[/]"

        table.add_row(
            str(s.stage_id),
            s.stage_name,
            str(s.tasks.task_count),
            duration_str,
            shuffle_str,
            spill_str,
            gc_str,
            input_str,
        )

    console.print(table)


def print_analysis_result(
    console: Console,
    result: AnalysisResult,
    *,
    use_ai: bool = True,
    output_config: Path | None = None,
) -> None:
    _print_rule_results(console, result.rule_results)

    if result.ai_report:
        _print_ai_report(console, result.ai_report)
        if output_config and result.ai_report.suggested_config:
            _save_config_file(console, output_config, result.ai_report)
    elif use_ai and not result.rule_results:
        console.print("[green]No issues found — skipping AI analysis.[/]")

    if not use_ai:
        console.print("[dim]AI analysis disabled. Set ANTHROPIC_API_KEY and remove --no-ai to enable.[/]")


def _print_rule_results(console: Console, results: list[RuleResult]) -> None:
    if not results:
        console.print("\n[green] No issues detected by rules engine[/]\n")
        return

    console.print("\n[bold]Issues Found[/]\n")

    for result in results:
        icon = SEVERITY_ICONS.get(result.severity, "")
        console.print(f"  {icon}: {result.title}")
        console.print(f"    {result.message}", style="dim")
        if result.recommended_value:
            console.print(f"    → {result.recommended_value}", style="green")
        console.print()


def _print_ai_report(console: Console, report: AdvisorReport) -> None:
    if not report.recommendations:
        return

    console.print(Panel(report.summary, title="[bold] AI Analysis[/]", border_style="magenta"))

    if report.causal_chain:
        console.print(f"\n[bold]Causal Chain:[/] {report.causal_chain}\n")

    table = Table(title="Recommendations", show_lines=True)
    table.add_column("#", style="bold", width=3)
    table.add_column("Recommendation", min_width=30)
    table.add_column("Change", min_width=25)
    table.add_column("Impact", min_width=20)

    for rec in report.recommendations:
        change = ""
        if rec.parameter and rec.current_value and rec.recommended_value:
            change = f"[red]{rec.parameter}\n{rec.current_value}[/] → [green]{rec.recommended_value}[/]"
        elif rec.recommended_value:
            change = f"[green]{rec.recommended_value}[/]"

        table.add_row(
            str(rec.priority),
            f"[bold]{rec.title}[/]\n{rec.explanation}",
            change,
            rec.estimated_impact,
        )

    console.print(table)

    if report.suggested_config:
        console.print("\n[bold]Suggested spark-defaults.conf:[/]\n")
        for key, value in report.suggested_config.items():
            console.print(f"  {key} = [green]{value}[/]")
        console.print()


def _save_config_file(console: Console, path: Path, report: AdvisorReport) -> None:
    path.write_text("\n".join(f"{k} = {v}" for k, v in report.suggested_config.items()) + "\n")
    console.print(f"[green]Config written to {path}[/]")
